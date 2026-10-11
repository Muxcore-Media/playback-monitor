package internal

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Muxcore-Media/core/sdk/go/module/erasure"
	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Confirm that the merge goroutine reached the contested mutex before a
// competing writer is released. A goroutine merely being launched does not
// establish overlap, especially on a busy CI runner.
func waitForMergeLock(t *testing.T, ctx context.Context, done <-chan error) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	stack := make([]byte, 256<<10)
	for {
		n := runtime.Stack(stack, true)
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, "(*Module).mergeUserIdentities(") && strings.Contains(goroutine, "sync.(*RWMutex).Lock(") {
				return
			}
		}
		select {
		case err := <-done:
			t.Fatalf("merge completed before waiting for the erasure lock: %v", err)
		case <-ctx.Done():
			t.Fatal("merge did not wait for the erasure lock: ", ctx.Err())
		case <-ticker.C:
		}
	}
}

func awaitErasureWork(t *testing.T, ctx context.Context, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		t.Fatal("erasure or merge did not finish: ", ctx.Err())
		return ctx.Err()
	}
}

// An acknowledged tombstone will be skipped on future sweeps. Merge must
// therefore refuse both erased endpoints before recreating any personal rows.
func TestErasureMergeRefusesAppliedEndpoint(t *testing.T) {
	for _, endpoint := range []string{"source", "target"} {
		t.Run(endpoint, func(t *testing.T) {
			clearErasureEnv(t)
			m := newErasureModule(t, "")
			h := newErasureHarness(t, m)
			mustIngest(t, m, nativeEvent("victim", "Victim", "one"))
			mustIngest(t, m, nativeEvent("bystander", "Bystander", "two"))
			h.provider.AddErasure("victim", "")
			r := h.reconciler(t)
			mustSweep(t, r)
			before := snapshotState(t, m)
			source, target := "victim", "bystander"
			if endpoint == "target" {
				source, target = target, source
			}
			_, err := m.MergeUserIdentity(context.Background(), &monitorv1.MergeUserIdentityRequest{
				SourceUserId: source, TargetUserId: target, TargetUserName: "Requested target",
			})
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("merge with erased %s: %v, want PermissionDenied", endpoint, err)
			}
			requireErased(t, before, snapshotState(t, m), nil, nil, nil)
			if next := mustSweep(t, r); next.Applied != 0 || next.Skipped != 1 {
				t.Errorf("next sweep = %+v, want the acknowledged erasure skipped", next)
			}
			remaining, err := m.ErasureOwner().(erasure.Verifier).Verify(context.Background(), erasure.Tombstone{UserID: "victim"})
			if err != nil || remaining != 0 {
				t.Errorf("post-condition after merge and next sweep = %d, %v", remaining, err)
			}
		})
	}
}

// Failed application still places the native user in the verified last-seen
// ledger. Both native endpoints and external aliases linked to that person's
// identity must be refused, without depending on the applied record.
func TestErasureMergeRefusesLastSeenEndpointsAndAliases(t *testing.T) {
	for _, tc := range []struct {
		name, source, target string
		legacySessionOnly    bool
	}{
		{"native_source", "victim", "bystander", false},
		{"native_target", "bystander", "victim", false},
		{"linked_source", "external-victim", "bystander", false},
		{"linked_target", "bystander", "external-victim", false},
		{"legacy_source", "external-victim", "bystander", true},
		{"legacy_target", "bystander", "external-victim", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearErasureEnv(t)
			m := newErasureModule(t, "")
			h := newErasureHarness(t, m)
			mustIngest(t, m, nativeEvent("victim", "Victim", "one"))
			mustIngest(t, m, nativeEvent("bystander", "Bystander", "two"))
			mustIngest(t, m, serverEvent("jellyfin", "external-victim", "External", "three"))
			if _, err := m.mergeUserIdentities(context.Background(), "external-victim", "External", "victim", "Victim"); err != nil {
				t.Fatal(err)
			}
			if tc.legacySessionOnly {
				if _, err := m.exec(context.Background(), `DELETE FROM server_users WHERE server_id = ? AND external_user_id = ?`, nativeServerID, "victim"); err != nil {
					t.Fatal(err)
				}
			}
			h.provider.AddErasure("victim", "")
			r := h.reconciler(t)
			m.mu.Lock()
			m.erasureRec = r
			m.mu.Unlock()
			m.erasureHook = func(string) error { return errors.New("injected apply failure") }
			if result, err := sweepOnce(t, r); err == nil || result.Failed != 1 {
				t.Fatalf("failed application = %+v, %v", result, err)
			}
			m.erasureHook = nil
			if !r.Erased("victim") || appliedCount(t, m) != 0 {
				t.Fatal("expected a last-seen tombstone with no applied record")
			}
			before := snapshotState(t, m)
			if _, err := m.mergeUserIdentities(context.Background(), tc.source, "", tc.target, "Target"); !errors.Is(err, errUserErased) {
				t.Errorf("merge: %v, want errUserErased", err)
			}
			requireErased(t, before, snapshotState(t, m), nil, nil, nil)
			if result := mustSweep(t, r); result.Applied != 1 {
				t.Fatalf("retry = %+v", result)
			}
		})
	}
}

// An ingest holds a read lock from its tombstone check through all writes.
// Merge must acquire the exclusive lock before looking up either identity.
func TestErasureMergeWaitsForIngest(t *testing.T) {
	clearErasureEnv(t)
	m := newErasureModule(t, "")
	mustIngest(t, m, nativeEvent("source", "Source", "one"))
	mustIngest(t, m, nativeEvent("target", "Target", "two"))
	before := snapshotState(t, m)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.erasureMu.RLock()
	locked := true
	defer func() {
		if locked {
			m.erasureMu.RUnlock()
		}
	}()
	done := make(chan error, 1)
	go func() {
		_, err := m.mergeUserIdentities(ctx, "source", "Source", "target", "Target")
		done <- err
	}()
	waitForMergeLock(t, ctx, done)
	// Read-only queries remain available while the merge waits for the lock.
	requireErased(t, before, snapshotState(t, m), nil, nil, nil)
	m.erasureMu.RUnlock()
	locked = false
	if err := awaitErasureWork(t, ctx, done); err != nil {
		t.Fatal(err)
	}
}

func TestErasureMergeQueuedDuringApplyCannotResurrect(t *testing.T) {
	clearErasureEnv(t)
	m := newErasureModule(t, "")
	mustIngest(t, m, nativeEvent("victim", "Victim", "one"))
	mustIngest(t, m, nativeEvent("bystander", "Bystander", "two"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	m.erasureHook = func(stage string) error {
		if stage != erasureStageIdentities {
			return nil
		}
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	applied := make(chan error, 1)
	go func() {
		_, err := m.ErasureOwner().Apply(ctx, erasure.Tombstone{ErasureID: "er-victim", UserID: "victim"})
		applied <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("apply did not reach identity lookup")
	}
	merged := make(chan error, 1)
	go func() {
		_, err := m.mergeUserIdentities(ctx, "bystander", "Bystander", "victim", "Victim")
		merged <- err
	}()
	waitForMergeLock(t, ctx, merged)
	close(release)
	if err := awaitErasureWork(t, ctx, applied); err != nil {
		t.Fatal(err)
	}
	if err := awaitErasureWork(t, ctx, merged); !errors.Is(err, errUserErased) {
		t.Errorf("queued merge: %v, want errUserErased", err)
	}
	remaining, err := m.ErasureOwner().(erasure.Verifier).Verify(ctx, erasure.Tombstone{UserID: "victim"})
	if err != nil || remaining != 0 {
		t.Errorf("post-condition after queued merge = %d, %v", remaining, err)
	}
	var survivors int
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM sessions WHERE user_id = 'bystander' AND user_name = 'Bystander'`).Scan(&survivors); err != nil || survivors != 1 {
		t.Errorf("bystander rows = %d, %v", survivors, err)
	}
}

// Hold the sole SQLite connection to pause a merge after it has acquired the
// erasure lock. An erasure arriving then must see the completed merge, so it
// deletes the whole linked identity while preserving unrelated accounts.
func TestErasureMergeInFlightCompletesBeforeApply(t *testing.T) {
	clearErasureEnv(t)
	m := newErasureModule(t, "")
	mustIngest(t, m, nativeEvent("victim", "Victim", "one"))
	mustIngest(t, m, serverEvent("jellyfin", "external-victim", "External", "two"))
	bystander := mustIngest(t, m, nativeEvent("bystander", "Bystander", "three"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := m.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	merged := make(chan error, 1)
	go func() {
		_, mergeErr := m.mergeUserIdentities(ctx, "external-victim", "External", "victim", "Victim")
		merged <- mergeErr
	}()
	for m.erasureMu.TryRLock() {
		m.erasureMu.RUnlock()
		select {
		case <-ctx.Done():
			t.Fatal("merge did not acquire the erasure lock")
		case <-time.After(time.Millisecond):
		}
	}
	applied := make(chan error, 1)
	go func() {
		_, applyErr := m.ErasureOwner().Apply(ctx, erasure.Tombstone{ErasureID: "er-victim", UserID: "victim"})
		applied <- applyErr
	}()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := awaitErasureWork(t, ctx, merged); err != nil {
		t.Fatal(err)
	}
	if err := awaitErasureWork(t, ctx, applied); err != nil {
		t.Fatal(err)
	}
	after := snapshotState(t, m)
	if len(after.sessions) != 1 || after.sessions[bystander] == "" || len(after.links) != 1 || len(after.identities) != 1 {
		t.Errorf("expected only the bystander after merge then erasure: %+v", after)
	}
}

func TestErasureMergeAmbiguousExternalIDRefusedButIngestAllowed(t *testing.T) {
	clearErasureEnv(t)
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	mustIngest(t, m, nativeEvent("victim", "Victim", "one"))
	mustIngest(t, m, serverEvent("jellyfin", "victim", "External", "two"))
	mustIngest(t, m, serverEvent("jellyfin", "other", "Other", "three"))
	h.provider.AddErasure("victim", "")
	mustSweep(t, h.reconciler(t))
	before := snapshotState(t, m)
	for _, pair := range [][2]string{{"victim", "other"}, {"other", "victim"}} {
		if _, err := m.mergeUserIdentities(context.Background(), pair[0], "", pair[1], ""); !errors.Is(err, errUserErased) {
			t.Errorf("ambiguous bare-id merge %v: %v", pair, err)
		}
	}
	requireErased(t, before, snapshotState(t, m), nil, nil, nil)
	mustIngest(t, m, serverEvent("jellyfin", "victim", "External", "later"))
	if _, err := m.mergeUserIdentities(context.Background(), "other", "Other", "new-external", "New"); err != nil {
		t.Fatalf("ordinary external merge after erasure: %v", err)
	}
}

// The legacy merge UPDATE also selects sessions whose user_id matches the
// source, even when lookupIdentityForUser resolved another server's identity.
// Refuse a native victim reached through those additional rows as well.
func TestErasureMergeChecksAllIdentitiesMatchedBySource(t *testing.T) {
	clearErasureEnv(t)
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	ctx := context.Background()
	mustIngest(t, m, nativeEvent("victim", "Victim", "one"))
	mustIngest(t, m, serverEvent("jellyfin-one", "alias", "Alias", "two"))
	if _, err := m.mergeUserIdentities(ctx, "victim", "Victim", "alias", "Alias"); err != nil {
		t.Fatal(err)
	}
	mustIngest(t, m, serverEvent("jellyfin-two", "alias", "Unrelated alias", "three"))
	// Make the newer external identity the deterministic legacy lookup result.
	if _, err := m.exec(ctx, `UPDATE server_users SET last_seen_at = '2099-01-01T00:00:00Z' WHERE server_id = 'jellyfin-two'`); err != nil {
		t.Fatal(err)
	}
	h.provider.AddErasure("victim", "")
	r := h.reconciler(t)
	m.mu.Lock()
	m.erasureRec = r
	m.mu.Unlock()
	m.erasureHook = func(string) error { return errors.New("injected apply failure") }
	if result, err := sweepOnce(t, r); err == nil || result.Failed != 1 {
		t.Fatalf("failed application = %+v, %v", result, err)
	}
	m.erasureHook = nil
	before := snapshotState(t, m)
	if _, err := m.mergeUserIdentities(ctx, "alias", "Alias", "new-target", "New"); !errors.Is(err, errUserErased) {
		t.Errorf("merge of additional matched identity: %v", err)
	}
	requireErased(t, before, snapshotState(t, m), nil, nil, nil)
	if result := mustSweep(t, r); result.Applied != 1 {
		t.Fatalf("retry = %+v", result)
	}
	var survivors int
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM sessions WHERE server_id = 'jellyfin-two' AND user_id = 'alias'`).Scan(&survivors); err != nil || survivors != 1 {
		t.Errorf("unrelated external alias rows = %d, %v", survivors, err)
	}
}

func TestErasureMergeMissingTargetDoesNotMatchUnrelatedLegacySession(t *testing.T) {
	clearErasureEnv(t)
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	ctx := context.Background()
	victim := mustIngest(t, m, nativeEvent("victim", "Victim", "one"))
	mustIngest(t, m, serverEvent("jellyfin", "source", "Source", "two"))
	if _, err := m.exec(ctx, `UPDATE sessions SET identity_id = '' WHERE id = ?`, victim); err != nil {
		t.Fatal(err)
	}
	h.provider.AddErasure("victim", "")
	r := h.reconciler(t)
	m.mu.Lock()
	m.erasureRec = r
	m.mu.Unlock()
	m.erasureHook = func(string) error { return errors.New("injected apply failure") }
	if result, err := sweepOnce(t, r); err == nil || result.Failed != 1 {
		t.Fatalf("failed application = %+v, %v", result, err)
	}
	m.erasureHook = nil
	before := snapshotState(t, m)
	if _, err := m.mergeUserIdentities(ctx, "source", "Source", "new-target", "New"); err != nil {
		t.Fatalf("unrelated merge to a missing target: %v", err)
	}
	if after := snapshotState(t, m); after.sessions[victim] != before.sessions[victim] {
		t.Error("unrelated legacy session changed")
	}
	if result := mustSweep(t, r); result.Applied != 1 {
		t.Fatalf("retry = %+v", result)
	}
}
