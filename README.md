# Playback Monitor

MuxCore module replacing **Tautulli / Tracearr analytics core**: session history, live activity, and home stats.

Module ID: `playback-monitor` · capability `playback.monitor`

## Features (v0.1.0)

- SQLite session store (started / progress / stopped lifecycle)
- Mesh subscription to `playback.started`, `playback.progress`, `playback.stopped`
- gRPC: `IngestSessionEvent`, `ListActiveSessions`, `ListHistory`, `GetHomeStats`
- HTTP: `/sessions/active`, `/history`, `/stats/home`, `/ingest`, `/healthz`

## Env

| Var | Default | Notes |
|-----|---------|-------|
| `PLAYBACK_MONITOR_DB_PATH` | `/var/lib/muxcore-playback-monitor/monitor.db` | Session DB |
| `PLAYBACK_MONITOR_GRPC_ADDR` | `:9560` | gRPC |
| `PLAYBACK_MONITOR_HTTP_ADDR` | `:8560` | HTTP |
| `MUXCORE_GRPC_ADDR` | | Core mesh for event subscription |

See [`TAUTULLI-TRACEARR-PARITY.md`](../TAUTULLI-TRACEARR-PARITY.md) for full module plan.
