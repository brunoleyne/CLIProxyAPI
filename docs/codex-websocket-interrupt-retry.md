# Codex recovery after an upstream interrupt connection is lost

## Symptom and cause

Codex 0.169.0 stops a turn with `400 invalid_request_error` and the message
`no active upstream websocket for response.interrupt`. The gateway emits that
status when an interrupt finds no active upstream and cannot be claimed by an
HTTP fallback turn. A missing transport is a transient failure, but Codex treats
the generic HTTP 400 as a terminal invalid request.

The production baseline examined on 2026-11-15 was CLIProxyAPI v8.0.72,
commit `e96d4c48`. Increasing Codex's retry count cannot repair that
classification. Disabling WebSockets is unnecessary.

## Change

After preserving local HTTP cancellation precedence, handle only
`ErrNoActiveUpstreamWebsocket` (including wrapped errors) as HTTP 503 with
`server_error` and code `upstream_websocket_disconnected`. Send one terminal
error and close the downstream connection atomically, then cancel the socket
context. Codex can reconnect and replay its generating request using its normal
bounded retry mechanism. The gateway does not redial or replay the interrupt.

Malformed interrupt IDs and interrupts rejected by a healthy active WebSocket
keep HTTP 400. Completed-response interrupts remain idempotent, and valid HTTP
fallback cancellation remains local. Error output uses a fixed message rather
than arbitrary wrapped transport details.

## Validation

- Focused handler regressions exercise disconnected, wrapped-error, idle, active
  WebSocket, completed response, HTTP fallback, and malformed interrupt paths.
- New reader tests require one 503, connection closure, context cancellation,
  an empty input queue, and absence of reflected secrets or fabricated
  `response.incomplete` events.
- An isolated localhost server and the installed Codex 0.169.0 reproduced the
  original 400 failure (one connection, exit 1). Injecting the new error shape
  made Codex reconnect, replay `response.create`, and finish successfully
  (two connections, exit 0). No real provider was used for fault injection.
- The complete OpenAI handler, runtime executor, and session packages pass.
  Focused interrupt, upstream-loss, and queue-recovery tests also pass with
  Go's race detector. Linux AMD64 and ARM64 builds succeed; the ARM64 build
  uses Go 1.26.10, matching the deployed release.
- The full repository suite was run. It exposed a separate pluginhost
  `Retry-After` assertion (`10`, expected `7`), reproduced unchanged in a
  pristine v8.0.72 source archive. A session performance ratio assertion failed
  during the parallel suite but passed in both the pristine isolated test and
  the final changed-source package run. Neither subsystem was changed here.

Production activation requires a gateway service restart. This source change
does not alter live traffic by itself. Preserve the original release for
rollback, and prevent the daily updater from replacing the local hotfix until
an official release includes the same recovery behavior.

## Relevant client behavior

- [WebSocket error classification](https://github.com/openai/codex/blob/rust-v0.169.0/codex-rs/codex-api/src/endpoint/responses_websocket.rs)
- [HTTP 400 mapping](https://github.com/openai/codex/blob/rust-v0.169.0/codex-rs/codex-api/src/api_bridge.rs)
- [Retry eligibility](https://github.com/openai/codex/blob/rust-v0.169.0/codex-rs/protocol/src/error.rs)
