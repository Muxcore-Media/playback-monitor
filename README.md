# Playback Monitor

MuxCore module replacing **Tautulli / Tracearr analytics core**: session history, live streams, library analytics, notification rules, and a Tracearr-compatible public read API.

Module ID: `playback-monitor` · capabilities: `playback.monitor`, `playback.analytics`, `settings`

## Storage

| Var | Default | Notes |
|-----|---------|-------|
| `PLAYBACK_MONITOR_DB_PATH` | `/var/lib/muxcore-playback-monitor/monitor.db` | SQLite file when Postgres URL unset |
| `PLAYBACK_MONITOR_DATABASE_URL` | — | `postgres://` / `postgresql://` URL (overrides SQLite) |
| `DATABASE_URL` | — | Alias for Postgres URL |
| `PLAYBACK_MONITOR_TIMESCALE` | unset | When set with Postgres, creates a Timescale hypertable on `sessions.started_at` |
| `PLAYBACK_MONITOR_HISTORY_RETENTION_DAYS` | `0` | Delete stopped sessions older than N days (`0` = keep forever). Sweeper runs daily. |

Durable operator settings (notify flags, geoip, public API key, retention) persist to `{db_dir}/settings.json`.

## Network

| Var | Default | Notes |
|-----|---------|-------|
| `PLAYBACK_MONITOR_GRPC_ADDR` | `:9560` | gRPC (`PlaybackMonitorService`) |
| `PLAYBACK_MONITOR_HTTP_ADDR` | `:8560` | Operator HTTP + public v2 |
| `PLAYBACK_MONITOR_HTTP_TOKEN` | unset | **Required** for operator HTTP (`Authorization: Bearer …` or `X-Playback-Monitor-Token`). Routes return `503` when unset. |
| `PLAYBACK_MONITOR_PUBLIC_API_KEY` | unset | Bearer token for `/api/v2/public/*` (`503` when unset) |
| `MUXCORE_GRPC_ADDR` | unset | Core mesh for playback/library/guard event subscription |
| `MUXCORE_INSECURE_DISABLE_TLS` | unset | Dev-only: disable TLS for mesh + peer gRPC dials |
| `PLAYBACK_MONITOR_ACTIVE_TIMEOUT` | `15m` | Mark `playing`/`paused` sessions stopped when `last_progress_at` is older than this |

## GeoIP

| Var | Default | Notes |
|-----|---------|-------|
| `PLAYBACK_MONITOR_GEOIP_DB` | unset | MaxMind GeoLite2 `.mmdb` path — **default lookup** when set |
| `PLAYBACK_MONITOR_GEOIP` | unset | Set to `plex` to use `https://plex.tv/api/v2/geoip` instead (**sends client IPs off-box**) |

## Notifications

| Var | Default | Notes |
|-----|---------|-------|
| `PLAYBACK_MONITOR_NOTIFY_ON_START` | `0` | Legacy notify on session start |
| `PLAYBACK_MONITOR_NOTIFY_ON_STOP` | `0` | Legacy notify on session stop |
| `PLAYBACK_MONITOR_ALLOW_LOCAL_WEBHOOKS` | `0` | Allow notification destination webhooks to `127.0.0.1` / RFC1918 hosts |

## gRPC (`PlaybackMonitorService`)

Session ingest and analytics: `IngestSessionEvent`, `ListActiveSessions`, `ListHistory`, `GetHomeStats`, `GetItemWatchStats`, `ListWatchUsers`, `GetStreamAnalytics`, `ListUserWatchStats`, charts (`GetPlaysByDate`, `GetPlaysByHour`, …), `GetConcurrentStreams`, server registry, `MergeUserIdentity`, imports (`ImportTautulliHistory`, `ImportJellystatHistory`), library analytics, `DeleteUserHistory(identity_id)`.

Proto: `proto/monitorv1/monitor.proto`

## HTTP surfaces

### Operator (requires `PLAYBACK_MONITOR_HTTP_TOKEN`)

- Health: `GET /healthz` (DB ping; `503` when DB down)
- Ingest: `POST /ingest`
- Sessions/history: `GET /sessions/active`, `GET /history`
- Stats/charts: `GET /stats/*`
- Library: `GET /library/*`
- Notifications: `GET|POST|PUT|DELETE /notification/*`
- Imports: `POST /import/tautulli`, `POST /import/jellystat`
- Live SSE: `GET /events/streams`

### Public v2 (requires `PLAYBACK_MONITOR_PUBLIC_API_KEY`)

Tracearr-shaped read API under `/api/v2/public/*` (`/health`, `/streams`, `/history`, `/stats/*`, `/users/*`, `/media/*`, `/libraries`, …). See `GET /api/v2/public/docs`.

## admin-ui

Admin playback UI proxies operator HTTP via authenticated routes:

- `/streams` — live dashboard
- `/streams/history`, `/streams/stats`, `/streams/libraries`, `/streams/users`, `/streams/servers`, `/streams/map`
- `/streams/guard`, `/streams/notifications`
- `/streams/events` — SSE (admin session auth)

## Mesh events consumed

- `playback.started`, `playback.progress`, `playback.stopped`
- `playback.library.item`
- `playback.guard.violation`
- `media.request.ready` — user-requested title became playable (`has_file`). Dispatched to matching Discord/webhook destinations; **deduped by `request_id`**. Emitted by **request-media** when a linked library item gains a file (not `status=available`). Unconfigured destinations are a quiet no-op.

Subscriptions reconnect automatically after core restarts.

## Build

```bash
cd playback-monitor
nix-shell -p go golangci-lint --run 'export GOCACHE=/tmp/gocache-playback-monitor; go test ./...'
```
