# Upgrade snapshots (ADR-0015)

| File | Produced by | Notes |
|------|-------------|-------|
| `v0.1.6.db` | `playback-monitor` tag `v0.1.6` (the release this slice upgrades from, ADR-0015 section 4) | `Init`, `ingestSessionEvent` and `mergeUserIdentities` of that tag seed the rows; no `erasure_applied` table |
| `v0.1.6.schema.sql` | `sqlite_master` of `v0.1.6.db` (same text as `sqlite3 v0.1.6.db .schema`) | schema of that tag |
| `seed_upgrade_test.go.txt` | seed test (build tag `upgradeseed`), stored as `.txt` so it does not compile here | |

The database is stored in rollback-journal mode (single file, 1 KiB pages);
the module switches it to WAL on open.

## How it was produced

`scripts/upgrade-fixtures/snapshot.sh` needs the `sqlite3` CLI, which was not
installed where this slice was built, so the seed test performs the
`journal_mode`/`VACUUM`/`.schema` steps itself:

```sh
git worktree add --detach /tmp/pm-v0.1.6 v0.1.6
cp internal/testdata/upgrade/seed_upgrade_test.go.txt /tmp/pm-v0.1.6/internal/zz_seed_upgrade_test.go
cd /tmp/pm-v0.1.6
GOWORK=off UPGRADE_SEED_DB=$PWD/v0.1.6.db UPGRADE_SEED_SCHEMA=$PWD/v0.1.6.schema.sql \
  go test -count=1 -tags upgradeseed -run '^TestUpgradeSeed$' ./internal/
```

## Seed summary

| Table | Rows | Notes |
|-------|------|-------|
| `sessions` | 6 | 2 native `u-victim` (one stopped), 1 native `u-bystander`, 1 `jellyfin-main/jf-victim` (merged into `u-victim`), 1 `jellyfin-other/u-victim` (same id string, different server), 1 `jellyfin-main/jf-solo` |
| `server_users` | 5 | one per account |
| `user_identities` | 4 | the merged Jellyfin account's own identity was removed by the merge |
