package internal

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Muxcore-Media/core/sdk/go/module/erasure"
	"github.com/Muxcore-Media/core/sdk/go/module/moduletest"
)

// upgradeSnapshots lists committed snapshots produced by the named tag's own
// code (ADR-0015). See testdata/upgrade/README.md.
var upgradeSnapshots = []string{"v0.1.6"}

func openUpgradeModule(t *testing.T, dbPath string) *Module {
	t.Helper()
	m := NewModule(Config{DBPath: dbPath, GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"})
	if err := m.Init(context.Background()); err != nil {
		t.Fatalf("Init(%s): %v", dbPath, err)
	}
	return m
}

func countUpgradeRows(t *testing.T, m *Module, table string) int {
	t.Helper()
	var n int
	if err := m.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// TestUpgradeFromSnapshots opens each committed snapshot with the current
// code twice (the first open upgrades, the second proves startup against an
// upgraded database is idempotent) and asserts the ADR-0015 contract plus the
// ADR-0035 table.
func TestUpgradeFromSnapshots(t *testing.T) {
	clearErasureEnv(t)
	for _, tag := range upgradeSnapshots {
		t.Run(tag, func(t *testing.T) {
			fresh := openUpgradeModule(t, filepath.Join(t.TempDir(), "fresh.db"))
			defer closeErasureModule(t, fresh)
			freshSchema := moduletest.Schema(t, fresh.db)

			path := moduletest.CopyFixture(t, filepath.Join("testdata", "upgrade", tag+".db"))
			for pass := 1; pass <= 2; pass++ {
				m := openUpgradeModule(t, path)
				upSchema := moduletest.Schema(t, m.db)
				moduletest.RequireSchemaSuperset(t, upSchema, freshSchema)
				requireUpgradedDefaults(t, upSchema, freshSchema)
				if _, ok := upSchema.Tables["erasure_applied"]; !ok {
					t.Fatal("erasure_applied was not created by the upgrade")
				}
				requireSnapshotRows(t, m)
				moduletest.RequireIntegrity(t, m.db)
				closeErasureModule(t, m)
			}
		})
	}
}

// requireUpgradedDefaults asserts that every column the current code defines
// has the same NOT NULL and DEFAULT contract after upgrade as in a fresh
// database, so columns added by ALTER TABLE get usable defaults.
func requireUpgradedDefaults(t *testing.T, upgraded, fresh moduletest.SchemaInfo) {
	t.Helper()
	for name, ft := range fresh.Tables {
		have := map[string]moduletest.Column{}
		for _, c := range upgraded.Tables[name].Columns {
			have[c.Name] = c
		}
		for _, fc := range ft.Columns {
			uc, ok := have[fc.Name]
			if !ok {
				continue // reported by RequireSchemaSuperset
			}
			if uc.HasDflt != fc.HasDflt || uc.Default != fc.Default || uc.NotNull != fc.NotNull {
				t.Errorf("column %s.%s: upgraded (notnull=%v default=%q has=%v), fresh (notnull=%v default=%q has=%v)",
					name, fc.Name, uc.NotNull, uc.Default, uc.HasDflt, fc.NotNull, fc.Default, fc.HasDflt)
			}
		}
	}
}

// requireSnapshotRows checks the seeded v0.1.6 rows read back and that the
// erasure table starts empty.
func requireSnapshotRows(t *testing.T, m *Module) {
	t.Helper()
	ctx := context.Background()
	for table, want := range map[string]int{
		"sessions": 6, "server_users": 5, "user_identities": 4, "erasure_applied": 0,
	} {
		if got := countUpgradeRows(t, m, table); got != want {
			t.Errorf("%s: %d rows after upgrade, want %d", table, got, want)
		}
	}
	hist, total, err := m.listHistory(ctx, nativeServerID, "u-victim", "", 10, 0)
	if err != nil || total != 1 || len(hist) != 1 {
		t.Fatalf("native victim history = %d/%d, %v", len(hist), total, err)
	}
	if h := hist[0]; h.IPAddress != "10.1.2.3" || h.Device != "Pixel 9" || h.UserName != "Victoria" {
		t.Errorf("native victim session = %+v", h)
	}
	active, err := m.listActiveSessions(ctx, "", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 5 { // 6 sessions, one stopped
		t.Errorf("%d active sessions, want 5", len(active))
	}
}

// TestUpgradedSnapshotErasure applies an erasure to the upgraded v0.1.6
// database: the merged identity is removed, the same-string external account
// and everyone else stay, the applied record is written and the integrity
// check still passes.
func TestUpgradedSnapshotErasure(t *testing.T) {
	clearErasureEnv(t)
	path := moduletest.CopyFixture(t, filepath.Join("testdata", "upgrade", "v0.1.6.db"))
	m := openUpgradeModule(t, path)
	defer closeErasureModule(t, m)
	ctx := context.Background()

	counts, err := m.ErasureOwner().Apply(ctx, erasure.Tombstone{ErasureID: "er-upgrade-1", UserID: "u-victim"})
	if err != nil {
		t.Fatalf("Apply on upgraded snapshot: %v", err)
	}
	want := erasure.Counts{"sessions": 3, "server_users": 2, "user_identities": 1}
	for k, v := range want {
		if counts[k] != v {
			t.Errorf("counts[%s] = %d, want %d (%v)", k, counts[k], v, counts)
		}
	}
	for table, want := range map[string]int{"sessions": 3, "server_users": 3, "user_identities": 3, "erasure_applied": 1} {
		if got := countUpgradeRows(t, m, table); got != want {
			t.Errorf("%s: %d rows after erasure, want %d", table, got, want)
		}
	}
	var survivors int
	if err := m.db.QueryRowContext(ctx, `
		SELECT COUNT(1) FROM sessions WHERE
			(server_id = 'jellyfin-other' AND user_id = 'u-victim') OR user_id IN ('u-bystander', 'jf-solo')`).Scan(&survivors); err != nil || survivors != 3 {
		t.Errorf("survivors = %d, %v; want 3", survivors, err)
	}
	if rem, err := m.ErasureOwner().(erasure.Verifier).Verify(ctx, erasure.Tombstone{UserID: "u-victim"}); err != nil || rem != 0 {
		t.Errorf("post-condition = %d, %v", rem, err)
	}
	moduletest.RequireIntegrity(t, m.db)
}
