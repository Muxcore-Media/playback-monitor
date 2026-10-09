package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"

	discoveryv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/discovery/v1"
	"github.com/Muxcore-Media/core/sdk/go/module/erasure"
)

// User erasure (ADR-0035, roadmap T-M4-07 slice E5).
//
// The identity provider's erasure ledger is the only authority. Nothing in
// this file reacts to an event, a header or an HTTP request: the only way
// data is erased is erasure.Reconciler -> erasureOwner.Apply, and the
// reconciler reads the ledger only from the verified identity provider.
//
// Disposition (ADR-0035 section 3), for a tombstone with user id U:
//
//	DELETE  the server_users row (muxcore-native, U)
//	DELETE  the user_identities row it points to
//	DELETE  every session of that identity (an operator merge asserted it is
//	        the same person)
//	DELETE  the identity's other links: the other server_users rows that
//	        point to the same identity (accounts an operator merged into it)
//	DELETE  sessions with server_id = muxcore-native and user_id = U
//	RETAIN  external media-server accounts that are not merged with the
//	        MuxCore identity. They live in a different id space and nothing
//	        links them to U. Operators use DeleteUserHistory or a merge.
//
// All of it, and the erasure_applied record, happens in ONE transaction. The
// affected identities are computed BEFORE the first delete: server_users is
// the only link from U to its identity, so deleting it first would strand
// the identity and its sessions where a retry cannot find them.

// nativeServerID is the server id the BFF sends for native MuxCore players.
// It is the only id space in which an erasure tombstone's user id is
// meaningful here; external_user_id values of other servers are the media
// servers' own ids and are never matched against it.
const nativeServerID = "muxcore-native"

// errUserErased is returned by ingest paths for a native user id that has
// been erased. It names no user.
var errUserErased = errors.New("playback-monitor: user has been erased; native sessions for this user are refused")

var errCoreNotConnected = errors.New("playback-monitor: core connection not established")

const (
	createErasureAppliedSQL = `CREATE TABLE IF NOT EXISTS erasure_applied (
		erasure_id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		tenant_id TEXT NOT NULL DEFAULT '',
		applied_at TEXT NOT NULL,
		counts_json TEXT NOT NULL DEFAULT '{}'
	)`
	createErasureAppliedUserIdxSQL = `CREATE INDEX IF NOT EXISTS erasure_applied_user_idx ON erasure_applied(user_id)`
)

// Stage names passed to Module.erasureHook (tests only).
const (
	erasureStageIdentities = "identities" // identities computed, nothing deleted yet
	erasureStageSessions   = "sessions"   // identity sessions deleted
	erasureStageLinks      = "links"      // server_users and identities deleted
	erasureStageNative     = "native"     // native sessions deleted
	erasureStageRecord     = "record"     // just before the applied row is written
)

// ensureErasureSchema creates the erasure_applied table. Unlike the legacy
// schema loop (which tolerates every statement error), a failure here is
// fatal: an erasure module that silently lacks its applied record would
// re-apply forever and could not refuse late ingest.
func (m *Module) ensureErasureSchema(ctx context.Context) error {
	for _, stmt := range []string{createErasureAppliedSQL, createErasureAppliedUserIdxSQL} {
		if _, err := m.exec(ctx, stmt); err != nil {
			return fmt.Errorf("erasure schema: %w", err)
		}
	}
	return nil
}

// erasureOwner adapts a Module to erasure.Owner and erasure.Verifier.
type erasureOwner struct{ m *Module }

var (
	_ erasure.Owner    = erasureOwner{}
	_ erasure.Verifier = erasureOwner{}
)

// ErasureOwner returns the module's ADR-0035 personal-data owner.
func (m *Module) ErasureOwner() erasure.Owner { return erasureOwner{m: m} }

// ModuleID implements erasure.Owner.
func (o erasureOwner) ModuleID() string { return o.m.id }

// Applied implements erasure.Owner: it reads the durable local record.
func (o erasureOwner) Applied(ctx context.Context, erasureID string) (bool, error) {
	if o.m.dbHandle() == nil {
		return false, errDBNotInitialized
	}
	var one int
	err := o.m.queryRow(ctx, `SELECT 1 FROM erasure_applied WHERE erasure_id = ?`, erasureID).Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, err
	}
}

// Apply implements erasure.Owner. See the file comment for the disposition.
// The tombstone's tenant is not checked: playback-monitor stores no tenant
// and the provider's user ids are globally unique (ADR-0035 section 3).
func (o erasureOwner) Apply(ctx context.Context, t erasure.Tombstone) (erasure.Counts, error) {
	m := o.m
	if t.ErasureID == "" || t.UserID == "" {
		return nil, erasure.WithDetail(erasure.DetailInvalidTombstone, errors.New("erasure id and user id are required"))
	}
	db := m.dbHandle()
	if db == nil {
		return nil, errDBNotInitialized
	}
	// Exclude ingest for the whole transaction so a session cannot be written
	// for the user between the identity lookup and the commit.
	m.erasureMu.Lock()
	defer m.erasureMu.Unlock()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Idempotent for an applied erasure id (also guards a concurrent apply).
	var prior string
	err = m.txQueryRow(ctx, tx, `SELECT counts_json FROM erasure_applied WHERE erasure_id = ?`, t.ErasureID).Scan(&prior)
	if err == nil {
		return countsFromJSON(prior), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	// 1. Compute the affected identities BEFORE deleting anything.
	identities, err := m.erasureIdentities(ctx, tx, t.UserID)
	if err != nil {
		return nil, err
	}
	if stageErr := m.erasureStage(erasureStageIdentities); stageErr != nil {
		return nil, stageErr
	}

	var nSessions, nLinks, nIdentities int64
	var n int64
	// 2. Sessions of every affected identity.
	for _, id := range identities {
		if n, err = m.txDelete(ctx, tx, `DELETE FROM sessions WHERE identity_id = ?`, id); err != nil {
			return nil, err
		}
		nSessions += n
	}
	if stageErr := m.erasureStage(erasureStageSessions); stageErr != nil {
		return nil, stageErr
	}
	// 3. The identities' links (server_users) and then the identities. The
	// links go first: server_users.identity_id references user_identities.
	for _, id := range identities {
		if n, err = m.txDelete(ctx, tx, `DELETE FROM server_users WHERE identity_id = ?`, id); err != nil {
			return nil, err
		}
		nLinks += n
	}
	for _, id := range identities {
		if n, err = m.txDelete(ctx, tx, `DELETE FROM user_identities WHERE id = ?`, id); err != nil {
			return nil, err
		}
		nIdentities += n
	}
	if stageErr := m.erasureStage(erasureStageLinks); stageErr != nil {
		return nil, stageErr
	}
	// 4. Native rows that no identity reached (legacy rows without an
	// identity_id), and the native link itself. Both are keyed by the
	// native server id; another server's external_user_id is never matched.
	if n, err = m.txDelete(ctx, tx, `DELETE FROM sessions WHERE server_id = ? AND user_id = ?`, nativeServerID, t.UserID); err != nil {
		return nil, err
	}
	nSessions += n
	if n, err = m.txDelete(ctx, tx, `DELETE FROM server_users WHERE server_id = ? AND external_user_id = ?`, nativeServerID, t.UserID); err != nil {
		return nil, err
	}
	nLinks += n
	if stageErr := m.erasureStage(erasureStageNative); stageErr != nil {
		return nil, stageErr
	}

	// 5. Record the application in the same transaction.
	counts := erasure.Counts{"sessions": nSessions, "server_users": nLinks, "user_identities": nIdentities}
	if stageErr := m.erasureStage(erasureStageRecord); stageErr != nil {
		return nil, stageErr
	}
	raw, err := json.Marshal(counts)
	if err != nil {
		return nil, err
	}
	if _, err = m.txExec(ctx, tx, `
		INSERT INTO erasure_applied(erasure_id, user_id, tenant_id, applied_at, counts_json)
		VALUES (?, ?, ?, ?, ?)`,
		t.ErasureID, t.UserID, t.TenantID, time.Now().UTC().Format(time.RFC3339), string(raw),
	); err != nil {
		return nil, err
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return nil, commitErr
	}
	return counts, nil
}

// Verify implements erasure.Verifier: the rows the disposition removes that
// still carry the user id. Identities cannot be recomputed after the fact, so
// this checks the two rows keyed by the id itself; dangling links are
// impossible because Apply deletes links and identity in one transaction.
func (o erasureOwner) Verify(ctx context.Context, t erasure.Tombstone) (int, error) {
	if o.m.dbHandle() == nil {
		return 0, errDBNotInitialized
	}
	var remaining int
	err := o.m.queryRow(ctx, `
		SELECT
			(SELECT COUNT(1) FROM server_users WHERE server_id = ? AND external_user_id = ?) +
			(SELECT COUNT(1) FROM sessions WHERE server_id = ? AND user_id = ?)`,
		nativeServerID, t.UserID, nativeServerID, t.UserID,
	).Scan(&remaining)
	return remaining, err
}

// erasureIdentities returns the identities the erasure reaches: the identity
// of the native link and any identity already carried by the user's native
// sessions. It must run before anything is deleted.
func (m *Module) erasureIdentities(ctx context.Context, tx *sql.Tx, userID string) ([]string, error) {
	var out []string
	seen := map[string]struct{}{}
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`SELECT identity_id FROM server_users WHERE server_id = ? AND external_user_id = ?`, []any{nativeServerID, userID}},
		{`SELECT DISTINCT identity_id FROM sessions WHERE server_id = ? AND user_id = ? AND identity_id != ''`, []any{nativeServerID, userID}},
	} {
		rows, err := m.txQueryRows(ctx, tx, q.sql, q.args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id sql.NullString
			if scanErr := rows.Scan(&id); scanErr != nil {
				_ = rows.Close()
				return nil, scanErr
			}
			if !id.Valid || id.String == "" {
				continue
			}
			if _, dup := seen[id.String]; !dup {
				seen[id.String] = struct{}{}
				out = append(out, id.String)
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (m *Module) txDelete(ctx context.Context, tx *sql.Tx, query string, args ...any) (int64, error) {
	res, err := m.txExec(ctx, tx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (m *Module) erasureStage(stage string) error {
	if m.erasureHook == nil {
		return nil
	}
	return m.erasureHook(stage)
}

func countsFromJSON(raw string) erasure.Counts {
	out := erasure.Counts{}
	var parsed map[string]int64
	if json.Unmarshal([]byte(raw), &parsed) == nil {
		for k, v := range parsed {
			out[k] = v
		}
	}
	return out
}

// refuseErased returns errUserErased for a native session of an erased user.
// It must be called with erasureMu held for reading.
//
// The durable erasure_applied record is the source of truth (it survives
// restarts). The reconciler's last-seen ledger additionally covers the window
// between listing a tombstone and applying it, and an apply that keeps
// failing (ADR-0035 section 3, last paragraph).
func (m *Module) refuseErased(ctx context.Context, serverID, userID string) error {
	if serverID != nativeServerID {
		return nil
	}
	uid := strings.TrimSpace(userID)
	if uid == "" {
		return nil
	}
	if rec := m.erasureReconciler(); rec != nil && rec.Erased(uid) {
		return errUserErased
	}
	var one int
	err := m.queryRow(ctx, `SELECT 1 FROM erasure_applied WHERE user_id = ? LIMIT 1`, uid).Scan(&one)
	switch {
	case err == nil:
		return errUserErased
	case errors.Is(err, sql.ErrNoRows):
		return nil
	default:
		return fmt.Errorf("check erasure record: %w", err)
	}
}

// coreCapabilityFinder answers the erasure dialer's identity-provider
// discovery through whichever core connection the module currently has, so
// reconnects need no new reconciler.
type coreCapabilityFinder struct{ m *Module }

func (f coreCapabilityFinder) FindByCapability(ctx context.Context, in *discoveryv1.FindByCapabilityRequest, opts ...grpc.CallOption) (*discoveryv1.FindByCapabilityResponse, error) {
	c := f.m.eventClient()
	if c == nil || c.Discovery == nil {
		return nil, errCoreNotConnected
	}
	return c.Discovery.Raw().FindByCapability(ctx, in, opts...)
}

// startErasureReconciler starts the ADR-0035 reconciler once, after the
// first core connection exists. It runs until Stop cancels it; Stop waits for
// it (stopErasureReconciler) before closing the database.
func (m *Module) startErasureReconciler(ctx context.Context) {
	m.mu.Lock()
	if m.erasureStarted || m.bgStopped {
		m.mu.Unlock()
		return
	}
	m.erasureStarted = true
	m.mu.Unlock()

	interval, err := erasure.IntervalFromEnv(nil)
	if err != nil {
		slog.Error("playback-monitor: invalid erasure sweep interval; using the default", "error", err, "default", erasure.DefaultInterval)
		interval = erasure.DefaultInterval
	}
	rec, err := erasure.New(erasure.Config{
		Owner:    m.ErasureOwner(),
		Dialer:   &erasure.ProviderDialer{Discovery: coreCapabilityFinder{m: m}},
		Interval: interval,
	})
	if err != nil {
		slog.Error("playback-monitor: erasure reconciler not started", "error", err)
		return
	}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.mu.Lock()
	if m.bgStopped {
		m.mu.Unlock()
		cancel()
		return
	}
	m.erasureRec, m.erasureCancel, m.erasureDone = rec, cancel, done
	m.mu.Unlock()
	go func() {
		defer close(done)
		if err := rec.Run(rctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("playback-monitor: erasure reconciler stopped", "error", err)
		}
	}()
}

// stopErasureReconciler cancels the reconciler and waits for it to exit.
func (m *Module) stopErasureReconciler() {
	m.mu.RLock()
	cancel, done := m.erasureCancel, m.erasureDone
	m.mu.RUnlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

func (m *Module) erasureReconciler() *erasure.Reconciler {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.erasureRec
}

func (m *Module) dbHandle() *sql.DB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db
}
