# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added
- ADR-0035 user erasure (T-M4-07 slice E5): an `erasure.Reconciler` (core v0.6.17 / sdk/go/module v0.6.7) applies the identity provider's erasure ledger in one transaction and records it in the new `erasure_applied` table (forward-only, idempotent migration). Deletes the `muxcore-native` account, its identity, the identity's sessions and other links; external media-server accounts that are not merged are retained. Native events for an erased user id are refused. `ERASURE_SWEEP_INTERVAL` sets the sweep interval.

## [0.1.6] - 2026-10-05


### Security
- NFR-SEC-009 / RULE-VAL-2: notification destination webhooks (discord/slack/webhook) must be https and may not target private, loopback, link-local or metadata addresses (netguard `UserURL`; checked at configure time and at dial/redirect time, which also closes DNS-rebinding). The `PLAYBACK_MONITOR_ALLOW_LOCAL_WEBHOOKS` bypass is removed. The Apprise base URL and the Tautulli import URL are admin-configured integrations: netguard `Integration` with LAN and loopback allowed, metadata/link-local blocked, via guarded clients (Tautulli no longer uses `http.DefaultClient`). Built on sdk/go/module v0.6.6.

## [0.1.5] - 2026-10-05


### Security
- gRPC server and peer dials use mesh TLS (meshtls, sdk/go/module v0.6.5) unless the dev insecure flag is set (ADR-0016/0017).

## [0.1.4] - 2026-10-05

### Changed
- Built on core v0.6.14 / sdk/go/module v0.6.4: unregisters on shutdown and re-registers after core restarts (ADR-0022).

## [0.1.3] - 2026-10-05


### Changed
- Reported version comes from muxcore.json (ADR-0021); built on core v0.6.12 / sdk/go/module v0.6.3 (mesh enrollment, ADR-0017).

## [0.1.2] - 2026-10-05


### Added

- `integsupport` package exposing `Module`, `Config`, `SessionEvent`, `NewTestModule`, `Start` and `IngestSession` for umbrella integration tests (T-M1-06, NFR-MNT-004).
