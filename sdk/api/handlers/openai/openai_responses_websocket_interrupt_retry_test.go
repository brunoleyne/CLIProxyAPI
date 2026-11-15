package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gorilla/websocket"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type interruptReaderTestSession struct {
	client *websocket.Conn
	ctx    context.Context
	input  <-chan cliproxyexecutor.WebsocketInput
	done   <-chan struct{}
}

func startInterruptReaderTestSession(t *testing.T, interrupt func([]byte) error, local *responsesLocalInterrupt) interruptReaderTestSession {
	t.Helper()
	ready := make(chan interruptReaderTestSession, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade reader websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := context.WithCancelCause(r.Context())
		defer cancel(nil)
		var epoch atomic.Uint64
		input, done := readResponsesWebsocketInput(ctx, cancel, conn, interrupt, local, newResponsesWebsocketWriter(conn), nil, &epoch)
		ready <- interruptReaderTestSession{ctx: ctx, input: input, done: done}
		<-done
	}))
	client, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if errDial != nil {
		server.Close()
		t.Fatal(errDial)
	}
	t.Cleanup(func() {
		_ = client.Close()
		server.Close()
	})
	if errDeadline := client.SetReadDeadline(time.Now().Add(3 * time.Second)); errDeadline != nil {
		t.Fatal(errDeadline)
	}
	select {
	case session := <-ready:
		session.client = client
		return session
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not start")
		return interruptReaderTestSession{}
	}
}

// Connection loss is a transport failure, not an invalid client request. Emit
// one retryable error and discard this socket so the next turn starts fresh.
func TestResponsesInterruptMissingUpstreamClosesForRetry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wrapped bool
		wsOwned bool
	}{
		{name: "idle"},
		{name: "wrapped", wrapped: true},
		{name: "websocket_turn", wsOwned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local := newResponsesLocalInterrupt()
			if tc.wsOwned {
				local.begin(true)
			}
			interruptErr := cliproxyexecutor.ErrNoActiveUpstreamWebsocket
			if tc.wrapped {
				interruptErr = fmt.Errorf("private transport at https://private-credential.example: %w", interruptErr)
			}
			session := startInterruptReaderTestSession(t, func([]byte) error { return interruptErr }, local)
			if errWrite := session.client.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.interrupt","response_id":"private-response-id","authorization":"private-token"}`)); errWrite != nil {
				t.Fatal(errWrite)
			}
			_, payload, errRead := session.client.ReadMessage()
			if errRead != nil {
				t.Fatalf("read retryable failure: %v", errRead)
			}
			if ojson.GetBytesNoCopy(payload, "type").String() != "error" ||
				ojson.GetBytesNoCopy(payload, "status").Int() != http.StatusServiceUnavailable ||
				ojson.GetBytesNoCopy(payload, "error.type").String() != "server_error" ||
				ojson.GetBytesNoCopy(payload, "error.code").String() != "upstream_websocket_disconnected" {
				t.Fatalf("expected retryable transport failure, got %s", payload)
			}
			if strings.Contains(string(payload), "private-") {
				t.Fatalf("transport details or client fields leaked: %s", payload)
			}
			if _, extra, errExtra := session.client.ReadMessage(); errExtra == nil {
				t.Fatalf("expected socket closure after one error, got %s", extra)
			}
			select {
			case <-session.done:
			case <-time.After(3 * time.Second):
				t.Fatal("reader did not stop after upstream loss")
			}
			if !errors.Is(context.Cause(session.ctx), cliproxyexecutor.ErrNoActiveUpstreamWebsocket) {
				t.Fatalf("socket context cause = %v", context.Cause(session.ctx))
			}
			if strings.Contains(context.Cause(session.ctx).Error(), "private-") {
				t.Fatalf("disconnect diagnostics retain private transport details: %v", context.Cause(session.ctx))
			}
			if message, ok := <-session.input; ok {
				t.Fatalf("interrupt was queued as a generating request: %s", message.Payload)
			}
			if frame := local.takeClaimed(); frame != nil {
				t.Fatalf("lost websocket turn claimed a local cancellation: %s", frame)
			}
		})
	}
}

func TestResponsesInterruptInvalidRequestKeepsSocket(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "missing_response_id", err: errors.New("response.interrupt requires response_id")},
		{name: "wrong_running_response", err: cliproxyexecutor.ErrUpstreamWebsocketInterruptRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := startInterruptReaderTestSession(t, func([]byte) error { return tc.err }, newResponsesLocalInterrupt())
			if errWrite := session.client.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.interrupt"}`)); errWrite != nil {
				t.Fatal(errWrite)
			}
			_, payload, errRead := session.client.ReadMessage()
			if errRead != nil {
				t.Fatal(errRead)
			}
			if ojson.GetBytesNoCopy(payload, "status").Int() != http.StatusBadRequest ||
				ojson.GetBytesNoCopy(payload, "error.type").String() != "invalid_request_error" {
				t.Fatalf("invalid interrupt must remain a 400: %s", payload)
			}
			const nextCreate = `{"type":"response.create","input":[]}`
			if errWrite := session.client.WriteMessage(websocket.TextMessage, []byte(nextCreate)); errWrite != nil {
				t.Fatal(errWrite)
			}
			select {
			case message, ok := <-session.input:
				if !ok || string(message.Payload) != nextCreate {
					t.Fatalf("same socket did not accept next create: %+v", message)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("valid next request was not delivered")
			}
			if session.ctx.Err() != nil {
				t.Fatalf("invalid interrupt canceled socket: %v", context.Cause(session.ctx))
			}
		})
	}
}

// A missing websocket is expected when the running turn uses HTTP. Its local
// cancellation must take precedence over the new reconnect path.
func TestResponsesInterruptHTTPFallbackDoesNotReconnect(t *testing.T) {
	local := newResponsesLocalInterrupt()
	local.begin(false)
	session := startInterruptReaderTestSession(t, func([]byte) error { return cliproxyexecutor.ErrNoActiveUpstreamWebsocket }, local)
	const interrupt = `{"type":"response.interrupt","response_id":"http-response"}`
	if errWrite := session.client.WriteMessage(websocket.TextMessage, []byte(interrupt)); errWrite != nil {
		t.Fatal(errWrite)
	}
	// A following create is a transport barrier proving the interrupt was handled.
	if errWrite := session.client.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","input":[]}`)); errWrite != nil {
		t.Fatal(errWrite)
	}
	select {
	case message, ok := <-session.input:
		if !ok || ojson.GetBytesNoCopy(message.Payload, "type").String() != "response.create" {
			t.Fatalf("HTTP interrupt closed or contaminated the reader: %+v", message)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reader stopped after HTTP interrupt")
	}
	if frame := local.takeClaimed(); string(frame) != interrupt {
		t.Fatalf("HTTP cancellation was not claimed: %s", frame)
	}
	if session.ctx.Err() != nil {
		t.Fatalf("HTTP interrupt canceled socket: %v", context.Cause(session.ctx))
	}
}
