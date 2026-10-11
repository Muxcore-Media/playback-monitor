package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Muxcore-Media/core/sdk/go/module/erasure"
	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	for _, tc := range []struct{ name, source, target string }{
		{"native_source", "victim", "bystander"},
		{"native_target", "bystander", "victim"},
		{"linked_source", "external-victim", "bystander"},
		{"linked_target", "bystander", "external-victim"},
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
	m.erasureMu.RLock()
	done := make(chan error, 1)
	go func() {
		_, err := m.mergeUserIdentities(context.Background(), "source", "Source", "target", "Target")
		done <- err
	}()
	select {
	case err := <-done:
		m.erasureMu.RUnlock()
		t.Fatalf("merge completed during ingest's read lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	// Read-only queries remain available while the merge waits for the lock.
	requireErased(t, before, snapshotState(t, m), nil, nil, nil)
	m.erasureMu.RUnlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("merge did not finish after ingest released its lock")
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
	close(release)
	if err := <-applied; err != nil {
		t.Fatal(err)
	}
	if err := <-merged; !errors.Is(err, errUserErased) {
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
