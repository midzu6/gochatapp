# gochatapp

WebSocket-based chat server written in Go. Built as a concurrency practice project — Hub/actor pattern, goroutines, channels, graceful shutdown.

## Architecture
```
client connects → HandleWS → Upgrade → NewClient → joinCh
                                                        ↓
                                                    Server.Run()
                                                        ↓
client sends message → ReadPump → broadcast → Run() → MessagesCh → WritePump → conn
                                                        ↓
client disconnects → ReadPump error → leaveCh → removeClient → close(MessagesCh)
```

## Components

**Server** — Hub that manages clients via channels. Runs a single `Run()` goroutine with a select loop for join, leave, and broadcast events.

**Client** — Wraps a WebSocket connection. Two goroutines per client:
- `ReadPump` — reads from conn, sends to broadcast
- `WritePump` — reads from MessagesCh, writes to conn, sends pings

**Graceful Shutdown** — `SIGINT/SIGTERM` triggers context cancellation → Run() closes all clients → HTTP server stops accepting connections → process exits cleanly.

## Stack

- `gorilla/websocket` — WebSocket implementation
- `google/uuid` — client IDs
- `testify` — assertions in tests
- `net/http`


## Run
```bash
go run ./cmd/main.go
```

## Test
```bash
go test ./... -race
```
