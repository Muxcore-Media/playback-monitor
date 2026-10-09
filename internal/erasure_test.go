package internal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
	authv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/auth/v1"
	discoveryv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/discovery/v1"
	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
	"github.com/Muxcore-Media/core/sdk/go/module/erasure"
	"github.com/Muxcore-Media/core/sdk/go/module/erasure/erasuretest"
	"github.com/Muxcore-Media/core/sdk/go/module/meshtls"
	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

const (
	erasureProviderID = "auth-local"
	erasureOwnerID    = "playback-monitor"
)

// clearErasureEnv removes every variable that changes the transport or the
// module's core connection, so each test states its environment explicitly.
func clearErasureEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"MUXCORE_INSECURE_DISABLE_TLS", "MUXCORE_DEV_TLS_SKIP", "MUXCORE_GRPC_INSECURE",
		meshtls.EnvTLSCert, meshtls.EnvTLSKey, meshtls.EnvTLSCA, meshtls.EnvTLSServerName,
		"MUXCORE_PROFILE", "MUXCORE_MESH_DIAL_LOCAL", "MUXCORE_GRPC_ADDR", "MUXCORE_MODULE_ID",
		erasure.EnvSweepInterval,
		"PLAYBACK_MONITOR_DATABASE_URL", "DATABASE_URL",
	} {
		t.Setenv(k, "")
	}
}

func newErasureModule(t *testing.T, dbPath string) *Module {
	t.Helper()
	if dbPath == "" {
		dbPath = filepath.Join(t.TempDir(), "monitor.db")
	}
	m := NewModule(Config{DBPath: dbPath, GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"})
	if err := m.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { closeErasureModule(t, m) })
	return m
}

func closeErasureModule(t *testing.T, m *Module) {
	t.Helper()
	if m.grpcLis != nil {
		_ = m.grpcLis.Close()
	}
	if m.httpLis != nil {
		_ = m.httpLis.Close()
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

// erasureHarness serves a fake auth-local over real mTLS and builds a
// reconciler for the module's real Owner.
type erasureHarness struct {
	m        *Module
	pki      *erasuretest.PKI
	provider *erasuretest.Provider
	disc     *erasuretest.Discovery
	dialer   *erasure.ProviderDialer
	addr     string
}

func newErasureHarness(t *testing.T, m *Module) *erasureHarness {
	t.Helper()
	clearErasureEnv(t)
	h := &erasureHarness{m: m, pki: erasuretest.NewPKI(t), provider: erasuretest.NewProvider()}
	h.provider.Allowed = map[string]bool{erasureOwnerID: true}
	sc, sk := h.pki.Issue(t, erasureProviderID)
	h.addr = erasuretest.ServeTLS(t, h.provider, sc, sk, h.pki.CAFile)
	h.disc = erasuretest.NewDiscovery(erasuretest.Module(erasureProviderID, h.addr))
	cc, ck := h.pki.Issue(t, erasureOwnerID)
	h.dialer = &erasure.ProviderDialer{Discovery: h.disc, CertFile: cc, KeyFile: ck, CAFile: h.pki.CAFile}
	return h
}

func (h *erasureHarness) reconciler(t *testing.T) *erasure.Reconciler {
	t.Helper()
	r, err := erasure.New(erasure.Config{
		Owner: h.m.ErasureOwner(), Dialer: h.dialer,
		Logger:   slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		Interval: time.Minute, AckBackoff: time.Millisecond, RetryBackoff: 10 * time.Millisecond,
		CallTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func sweepOnce(t *testing.T, r *erasure.Reconciler) (erasure.Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return r.SweepOnce(ctx)
}

func mustSweep(t *testing.T, r *erasure.Reconciler) erasure.Result {
	t.Helper()
	res, err := sweepOnce(t, r)
	if err != nil {
		t.Fatalf("sweep: %v (result %+v)", err, res)
	}
	return res
}

func wantAck(t *testing.T, p *erasuretest.Provider, erasureID string, outcome authv1.ErasureOutcome, detail string) erasuretest.Ack {
	t.Helper()
	a, ok := p.Latest(erasureID, erasureOwnerID)
	if !ok {
		t.Fatalf("no acknowledgement of %s", erasureID)
	}
	if a.Outcome != outcome || a.Detail != detail {
		t.Fatalf("ack = %v/%q, want %v/%q", a.Outcome, a.Detail, outcome, detail)
	}
	return a
}

// ---------------------------------------------------------------------------
// Seed data

func nativeEvent(user, name, ext string) SessionEvent {
	return SessionEvent{
		EventType: playbackevents.EventPlaybackStarted, SourceModule: "media-ui", ServerType: "native", ServerID: nativeServerID,
		ExternalSessionID: "native:default:" + user + ":" + ext, UserID: user, UserName: name,
		ItemID: "item-" + ext, Title: "Title " + ext, IPAddress: "10.1.2.3", Device: "Pixel 9", Player: "media-ui",
	}
}

func serverEvent(serverID, user, name, ext string) SessionEvent {
	return SessionEvent{
		EventType: playbackevents.EventPlaybackStarted, SourceModule: serverID, ServerType: "jellyfin", ServerID: serverID,
		ExternalSessionID: serverID + ":" + ext, UserID: user, UserName: name,
		ItemID: "item-" + ext, Title: "Title " + ext, IPAddress: "192.168.0.9", Device: "Living room TV",
	}
}

func mustIngest(t *testing.T, m *Module, ev SessionEvent) string {
	t.Helper()
	id, _, err := m.ingestSessionEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("ingest %s/%s: %v", ev.ServerID, ev.UserID, err)
	}
	return id
}

// table dumps are keyed by primary key so that "everything else is
// untouched" can be asserted exactly.
func dumpTable(t *testing.T, m *Module, query string) map[string]string {
	t.Helper()
	rows, err := m.db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("dump %q: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = v.String
		}
		out[parts[0]] = strings.Join(parts, "|")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

type dbState struct{ sessions, links, identities map[string]string }

func snapshotState(t *testing.T, m *Module) dbState {
	t.Helper()
	return dbState{
		sessions:   dumpTable(t, m, `SELECT id, server_id, external_session_id, user_id, user_name, identity_id, ip_address, device, title FROM sessions`),
		links:      dumpTable(t, m, `SELECT server_id || '/' || external_user_id, server_id, external_user_id, identity_id, user_name FROM server_users`),
		identities: dumpTable(t, m, `SELECT id, display_name FROM user_identities`),
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// requireErased asserts that exactly the wanted keys disappeared from each
// table and that every other row is byte-for-byte unchanged.
func requireErased(t *testing.T, before, after dbState, wantSessions, wantLinks, wantIdentities []string) {
	t.Helper()
	check := func(name string, b, a map[string]string, gone []string) {
		t.Helper()
		want := map[string]bool{}
		for _, k := range gone {
			if _, ok := b[k]; !ok {
				t.Fatalf("%s: test bug, %q was not present before", name, k)
			}
			want[k] = true
		}
		for k, row := range b {
			got, ok := a[k]
			switch {
			case want[k] && ok:
				t.Errorf("%s: %q survived the erasure: %s", name, k, got)
			case !want[k] && !ok:
				t.Errorf("%s: bystander %q was deleted: %s", name, k, row)
			case !want[k] && got != row:
				t.Errorf("%s: bystander %q changed: %s -> %s", name, k, row, got)
			}
		}
		for k := range a {
			if _, ok := b[k]; !ok {
				t.Errorf("%s: unexpected new row %q", name, k)
			}
		}
	}
	check("sessions", before.sessions, after.sessions, wantSessions)
	check("server_users", before.links, after.links, wantLinks)
	check("user_identities", before.identities, after.identities, wantIdentities)
}

func appliedCount(t *testing.T, m *Module) int {
	t.Helper()
	var n int
	if err := m.db.QueryRowContext(context.Background(), `SELECT COUNT(1) FROM erasure_applied`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func identityOf(t *testing.T, m *Module, serverID, ext string) string {
	t.Helper()
	var id string
	if err := m.db.QueryRowContext(context.Background(),
		`SELECT identity_id FROM server_users WHERE server_id = ? AND external_user_id = ?`, serverID, ext).Scan(&id); err != nil {
		t.Fatalf("identity of %s/%s: %v", serverID, ext, err)
	}
	return id
}

// world is a household: victim and bystander native users, a Jellyfin
// account merged into the victim, a second Jellyfin server whose account has
// the SAME external_user_id string as the victim's native id (the stale
// draft's bug), and an unrelated unmerged Jellyfin account.
type world struct {
	victimSessions   []string
	victimIdentity   string
	mergedSessions   []string
	collidingSession string
	soloSession      string
	bystanders       []string
}

func seedWorld(t *testing.T, m *Module) world {
	t.Helper()
	ctx := context.Background()
	var w world
	w.victimSessions = append(w.victimSessions,
		mustIngest(t, m, nativeEvent("u-victim", "Victoria", "s1")),
		mustIngest(t, m, nativeEvent("u-victim", "Victoria", "s2")))
	w.bystanders = append(w.bystanders,
		mustIngest(t, m, nativeEvent("u-bystander", "Barry", "s1")),
		mustIngest(t, m, nativeEvent("u-bystander", "Barry", "s2")))
	w.mergedSessions = append(w.mergedSessions,
		mustIngest(t, m, serverEvent("jellyfin-main", "jf-victim", "vicky", "a")),
		mustIngest(t, m, serverEvent("jellyfin-main", "jf-victim", "vicky", "b")))
	w.victimIdentity = identityOf(t, m, nativeServerID, "u-victim")

	// The operator asserts jf-victim is the same person as native u-victim.
	if _, err := m.mergeUserIdentities(ctx, "jf-victim", "vicky", "u-victim", "Victoria"); err != nil {
		t.Fatalf("merge: %v", err)
	}
	// Created after the merge on purpose: the merge helper resolves accounts by
	// external_user_id alone, so an earlier colliding row could be picked.
	w.collidingSession = mustIngest(t, m, serverEvent("jellyfin-other", "u-victim", "Somebody Else", "c"))
	w.soloSession = mustIngest(t, m, serverEvent("jellyfin-main", "jf-solo", "Solo", "d"))
	return w
}

// ---------------------------------------------------------------------------
// Disposition

func TestErasureNativeVictimKeepsBystander(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	v1 := mustIngest(t, m, nativeEvent("u-victim", "Victoria", "s1"))
	v2 := mustIngest(t, m, nativeEvent("u-victim", "Victoria", "s2"))
	mustIngest(t, m, nativeEvent("u-bystander", "Barry", "s1"))
	before := snapshotState(t, m)
	vi := identityOf(t, m, nativeServerID, "u-victim")

	id := h.provider.AddErasure("u-victim", "")
	res := mustSweep(t, h.reconciler(t))
	if res.Seen != 1 || res.Applied != 1 || res.Acked != 1 || res.Failed != 0 || res.Errors != 0 {
		t.Fatalf("result %+v", res)
	}
	requireErased(t, before, snapshotState(t, m),
		[]string{v1, v2}, []string{nativeServerID + "/u-victim"}, []string{vi})
	ack := wantAck(t, h.provider, id, authv1.ErasureOutcome_ERASURE_OUTCOME_OK, "")
	if ack.Counts["sessions"] != 2 || ack.Counts["server_users"] != 1 || ack.Counts["user_identities"] != 1 {
		t.Fatalf("counts %v", ack.Counts)
	}
	rem, err := m.ErasureOwner().(erasure.Verifier).Verify(context.Background(), erasure.Tombstone{ErasureID: id, UserID: "u-victim"})
	if err != nil || rem != 0 {
		t.Fatalf("post-condition = %d, %v; want 0", rem, err)
	}
	if ok, err := m.ErasureOwner().Applied(context.Background(), id); err != nil || !ok {
		t.Fatalf("Applied = %v, %v", ok, err)
	}
	// The applied record carries ids and counts, no personal data.
	var user, counts, appliedAt string
	if err := m.db.QueryRowContext(context.Background(),
		`SELECT user_id, counts_json, applied_at FROM erasure_applied WHERE erasure_id = ?`, id).Scan(&user, &counts, &appliedAt); err != nil {
		t.Fatal(err)
	}
	if user != "u-victim" || counts != `{"server_users":1,"sessions":2,"user_identities":1}` || appliedAt == "" {
		t.Fatalf("record = %q %q %q", user, counts, appliedAt)
	}
}

// A merged identity: the native account and a Jellyfin account the operator
// merged into it. Both accounts' history, the links and the identity go; an
// unmerged account on another server that merely reuses the id string, and an
// unrelated account, stay.
func TestErasureMergedIdentityDeletesPersonNotNameCollision(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	w := seedWorld(t, m)
	before := snapshotState(t, m)

	// Sanity: the merge really did attach the Jellyfin account to the victim.
	if got := identityOf(t, m, "jellyfin-main", "jf-victim"); got != w.victimIdentity {
		t.Fatalf("merge did not link jf-victim to the victim identity (%s != %s)", got, w.victimIdentity)
	}
	collidingIdentity := identityOf(t, m, "jellyfin-other", "u-victim")
	if collidingIdentity == w.victimIdentity {
		t.Fatal("test bug: colliding account shares the victim identity")
	}

	h.provider.AddErasure("u-victim", "")
	mustSweep(t, h.reconciler(t))

	gone := append(append([]string{}, w.victimSessions...), w.mergedSessions...)
	requireErased(t, before, snapshotState(t, m), gone,
		[]string{nativeServerID + "/u-victim", "jellyfin-main/jf-victim"}, []string{w.victimIdentity})

	// Explicit survivors, independent of the generic diff above.
	after := snapshotState(t, m)
	for _, id := range append([]string{w.collidingSession, w.soloSession}, w.bystanders...) {
		if _, ok := after.sessions[id]; !ok {
			t.Errorf("session %s was erased", id)
		}
	}
	for _, k := range []string{"jellyfin-other/u-victim", "jellyfin-main/jf-solo", nativeServerID + "/u-bystander"} {
		if _, ok := after.links[k]; !ok {
			t.Errorf("link %s was erased", k)
		}
	}
	if _, ok := after.identities[collidingIdentity]; !ok {
		t.Error("the colliding account's identity was erased")
	}
}

// Reverse merge direction: the native account is the merge SOURCE, so its
// sessions are rewritten to the Jellyfin account's user id and the only route
// from the native user id to the identity is the server_users link. This is
// the case that proves the identities are computed before server_users is
// deleted.
func TestErasureReverseMergeFindsIdentityThroughNativeLink(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	ctx := context.Background()
	n1 := mustIngest(t, m, nativeEvent("u-victim", "Victoria", "s1"))
	j1 := mustIngest(t, m, serverEvent("jellyfin-main", "jf-victim", "vicky", "a"))
	by := mustIngest(t, m, nativeEvent("u-bystander", "Barry", "s1"))
	if _, err := m.mergeUserIdentities(ctx, "u-victim", "Victoria", "jf-victim", "vicky"); err != nil {
		t.Fatal(err)
	}
	// After the merge no session carries the native user id any more.
	var withNativeID int
	if err := m.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM sessions WHERE server_id = ? AND user_id = ?`, nativeServerID, "u-victim").Scan(&withNativeID); err != nil || withNativeID != 0 {
		t.Fatalf("test bug: %d native sessions still carry the user id (%v)", withNativeID, err)
	}
	merged := identityOf(t, m, nativeServerID, "u-victim")
	if merged != identityOf(t, m, "jellyfin-main", "jf-victim") {
		t.Fatal("test bug: accounts not merged")
	}
	before := snapshotState(t, m)

	h.provider.AddErasure("u-victim", "")
	mustSweep(t, h.reconciler(t))

	requireErased(t, before, snapshotState(t, m), []string{n1, j1},
		[]string{nativeServerID + "/u-victim", "jellyfin-main/jf-victim"}, []string{merged})
	if _, ok := snapshotState(t, m).sessions[by]; !ok {
		t.Error("bystander session erased")
	}
}

// The stale draft matched external_user_id without server_id: an erasure for a
// MuxCore user id must not touch a media-server account whose id string is
// equal, with or without any merge.
func TestErasureDoesNotMatchOtherServersWithSameExternalID(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	v := mustIngest(t, m, nativeEvent("shared-id", "Native", "s1"))
	j := mustIngest(t, m, serverEvent("jellyfin-main", "shared-id", "Jelly", "a"))
	p := mustIngest(t, m, serverEvent("plex-main", "shared-id", "Plexy", "a"))
	before := snapshotState(t, m)
	vi := identityOf(t, m, nativeServerID, "shared-id")

	h.provider.AddErasure("shared-id", "")
	mustSweep(t, h.reconciler(t))

	requireErased(t, before, snapshotState(t, m), []string{v}, []string{nativeServerID + "/shared-id"}, []string{vi})
	after := snapshotState(t, m)
	for _, id := range []string{j, p} {
		if _, ok := after.sessions[id]; !ok {
			t.Errorf("same-string external account session %s was erased", id)
		}
	}
	for _, k := range []string{"jellyfin-main/shared-id", "plex-main/shared-id"} {
		if _, ok := after.links[k]; !ok {
			t.Errorf("same-string external account link %s was erased", k)
		}
	}
}

// Two native accounts an operator merged are one person for the ledger: erasing
// one erases the merged history of both (ADR-0035 section 3). The other
// account's own future events then start a fresh identity.
func TestErasureMergedNativeAccountsShareFate(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	a := mustIngest(t, m, nativeEvent("u-a", "A", "s1"))
	b := mustIngest(t, m, nativeEvent("u-b", "B", "s1"))
	c := mustIngest(t, m, nativeEvent("u-c", "C", "s1"))
	if _, err := m.mergeUserIdentities(context.Background(), "u-b", "B", "u-a", "A"); err != nil {
		t.Fatal(err)
	}
	merged := identityOf(t, m, nativeServerID, "u-a")
	before := snapshotState(t, m)

	h.provider.AddErasure("u-a", "")
	mustSweep(t, h.reconciler(t))

	requireErased(t, before, snapshotState(t, m), []string{a, b},
		[]string{nativeServerID + "/u-a", nativeServerID + "/u-b"}, []string{merged})
	if _, ok := snapshotState(t, m).sessions[c]; !ok {
		t.Error("unmerged native user erased")
	}
	// u-b is not itself erased: it can start again under a new identity.
	mustIngest(t, m, nativeEvent("u-b", "B", "s2"))
}

// Rows written before identities existed carry no identity_id and have no
// link; the native-session rule still reaches them.
func TestErasureReachesLegacyNativeSessionsWithoutIdentity(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	ctx := context.Background()
	v := mustIngest(t, m, nativeEvent("u-victim", "Victoria", "s1"))
	by := mustIngest(t, m, nativeEvent("u-bystander", "Barry", "s1"))
	if _, err := m.db.ExecContext(ctx, `UPDATE sessions SET identity_id = '' WHERE id = ?`, v); err != nil {
		t.Fatal(err)
	}
	if _, err := m.db.ExecContext(ctx, `DELETE FROM server_users WHERE server_id = ? AND external_user_id = ?`, nativeServerID, "u-victim"); err != nil {
		t.Fatal(err)
	}
	before := snapshotState(t, m)

	h.provider.AddErasure("u-victim", "")
	res := mustSweep(t, h.reconciler(t))
	if res.Failed != 0 {
		t.Fatalf("result %+v", res)
	}
	after := snapshotState(t, m)
	if _, ok := after.sessions[v]; ok {
		t.Fatal("legacy native session survived")
	}
	if _, ok := after.sessions[by]; !ok {
		t.Fatal("bystander erased")
	}
	// Only the victim's session disappears; links and identities are unchanged
	// (the victim's identity row was already orphaned by the setup above).
	requireErased(t, before, after, []string{v}, nil, nil)
}

// ---------------------------------------------------------------------------
// Transaction, idempotency, failure

func TestErasureSecondSweepIsNoOpAndApplyIsIdempotent(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	seedWorld(t, m)
	h.provider.AddErasure("u-victim", "")
	r := h.reconciler(t)
	first := mustSweep(t, r)
	if first.Applied != 1 {
		t.Fatalf("first %+v", first)
	}
	afterFirst := snapshotState(t, m)
	second := mustSweep(t, r)
	if second.Applied != 0 || second.Skipped != 1 || second.Errors != 0 {
		t.Fatalf("second sweep %+v, want a no-op", second)
	}
	requireErased(t, afterFirst, snapshotState(t, m), nil, nil, nil)
	if n := appliedCount(t, m); n != 1 {
		t.Fatalf("%d applied rows, want 1", n)
	}

	// Apply itself is idempotent for an applied erasure id.
	var erasureID string
	if err := m.db.QueryRowContext(context.Background(), `SELECT erasure_id FROM erasure_applied`).Scan(&erasureID); err != nil {
		t.Fatal(err)
	}
	counts, err := m.ErasureOwner().Apply(context.Background(), erasure.Tombstone{ErasureID: erasureID, UserID: "u-victim"})
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if counts["sessions"] != 4 {
		t.Fatalf("re-apply returned counts %v, want the original application's", counts)
	}
	requireErased(t, afterFirst, snapshotState(t, m), nil, nil, nil)
}

func TestErasureMidTransactionFailureRollsBackAndNextSweepCompletes(t *testing.T) {
	for _, stage := range []string{erasureStageIdentities, erasureStageSessions, erasureStageLinks, erasureStageNative, erasureStageRecord} {
		t.Run(stage, func(t *testing.T) {
			m := newErasureModule(t, "")
			h := newErasureHarness(t, m)
			w := seedWorld(t, m)
			before := snapshotState(t, m)
			id := h.provider.AddErasure("u-victim", "")
			r := h.reconciler(t)

			fail := true
			m.erasureHook = func(s string) error {
				if fail && s == stage {
					return errors.New("injected failure at " + s)
				}
				return nil
			}
			res, err := sweepOnce(t, r)
			if err == nil || res.Failed != 1 || res.Applied != 0 {
				t.Fatalf("failing sweep: %+v, %v", res, err)
			}
			// Nothing changed and nothing was recorded.
			requireErased(t, before, snapshotState(t, m), nil, nil, nil)
			if n := appliedCount(t, m); n != 0 {
				t.Fatalf("%d applied rows after a rolled-back apply", n)
			}
			wantAck(t, h.provider, id, authv1.ErasureOutcome_ERASURE_OUTCOME_FAILED, erasure.DetailApplyFailed)
			if ok, _ := m.ErasureOwner().Applied(context.Background(), id); ok {
				t.Fatal("Applied reports true after a rolled-back apply")
			}

			fail = false
			res = mustSweep(t, r)
			if res.Applied != 1 || res.Failed != 0 {
				t.Fatalf("recovery sweep %+v", res)
			}
			gone := append(append([]string{}, w.victimSessions...), w.mergedSessions...)
			requireErased(t, before, snapshotState(t, m), gone,
				[]string{nativeServerID + "/u-victim", "jellyfin-main/jf-victim"}, []string{w.victimIdentity})
			wantAck(t, h.provider, id, authv1.ErasureOutcome_ERASURE_OUTCOME_OK, "")
			rem, err := m.ErasureOwner().(erasure.Verifier).Verify(context.Background(), erasure.Tombstone{ErasureID: id, UserID: "u-victim"})
			if err != nil || rem != 0 {
				t.Fatalf("post-condition %d, %v", rem, err)
			}
		})
	}
}

func TestErasureIsRefusedBeforeApplyWhenTombstoneIsInvalid(t *testing.T) {
	m := newErasureModule(t, "")
	mustIngest(t, m, nativeEvent("u-victim", "V", "s1"))
	before := snapshotState(t, m)
	for _, tc := range []erasure.Tombstone{{ErasureID: "", UserID: "u-victim"}, {ErasureID: "er-1", UserID: ""}} {
		if _, err := m.ErasureOwner().Apply(context.Background(), tc); err == nil {
			t.Fatalf("Apply(%+v) succeeded", tc)
		}
	}
	requireErased(t, before, snapshotState(t, m), nil, nil, nil)
	if n := appliedCount(t, m); n != 0 {
		t.Fatalf("%d applied rows", n)
	}
}

// ---------------------------------------------------------------------------
// Authority

func TestErasureLedgerFromWrongCNIsNeverActedOn(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	seedWorld(t, m)
	before := snapshotState(t, m)

	// A same-CA module certificate for another module that even carries the
	// provider's name as a SAN: only the CN pin can refuse it.
	imp := erasuretest.NewProvider()
	imp.AddErasure("u-victim", "")
	ic, ik := h.pki.Issue(t, "request-media", erasureProviderID, "localhost")
	addr := erasuretest.ServeTLS(t, imp, ic, ik, h.pki.CAFile)
	h.disc.Set("identity", erasuretest.Module(erasureProviderID, addr))
	r := h.reconciler(t)
	if _, err := sweepOnce(t, r); err == nil {
		t.Fatal("a ledger served by the wrong CN was accepted")
	} else if !strings.Contains(err.Error(), erasure.ErrProviderIdentity.Error()) {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
	requireErased(t, before, snapshotState(t, m), nil, nil, nil)
	if n := appliedCount(t, m); n != 0 {
		t.Fatalf("%d applied rows", n)
	}
	if list, _ := imp.Calls(); list != 0 {
		t.Fatalf("impostor received %d ledger calls", list)
	}
	if r.Erased("u-victim") {
		t.Fatal("impostor ledger leaked into Erased()")
	}
	// ...and the guard does not refuse the user either: nothing was learned.
	mustIngest(t, m, nativeEvent("u-victim", "Victoria", "s9"))
}

func TestErasureProviderUnreachableErasesNothing(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	seedWorld(t, m)
	before := snapshotState(t, m)
	h.provider.AddErasure("u-victim", "")
	h.provider.SetListError(status.Error(codes.Unavailable, "provider down"))
	if _, err := sweepOnce(t, h.reconciler(t)); err == nil {
		t.Fatal("sweep succeeded with the provider down")
	}
	requireErased(t, before, snapshotState(t, m), nil, nil, nil)
}

// A bus event, whoever publishes it, cannot erase: the only erasure path is
// the verified ledger. The module has no handler that reads an identity event.
func TestErasureFakeBusEventCannotErase(t *testing.T) {
	clearErasureEnv(t)
	m := newErasureModule(t, "")
	seedWorld(t, m)
	before := snapshotState(t, m)
	ctx := context.Background()

	for _, typ := range []string{"identity.user.deleted", "identity.erasure.recorded", "auth.user.deleted"} {
		payload, _ := json.Marshal(map[string]string{"user_id": "u-victim", "erasure_id": "er-forged", "tenant_id": ""})
		evt := &eventsv1.Event{Id: "x", Type: typ, Source: erasureProviderID, Payload: payload}
		m.handlePlaybackEvent(ctx, typ, evt)
		m.handleLibraryItemEvent(ctx, evt)
		m.handleGuardViolationEvent(ctx, evt)
		m.handleRequestReadyEvent(ctx, evt)
	}
	// Whatever a malformed playback payload may have created, the victim's
	// rows are intact and nothing was recorded as erased.
	after := snapshotState(t, m)
	for k, row := range before.sessions {
		if after.sessions[k] != row {
			t.Errorf("session %s changed by a bus event: %s -> %s", k, row, after.sessions[k])
		}
	}
	for k, row := range before.links {
		if after.links[k] != row {
			t.Errorf("link %s changed by a bus event", k)
		}
	}
	for k := range before.identities {
		if _, ok := after.identities[k]; !ok {
			t.Errorf("identity %s deleted by a bus event", k)
		}
	}
	if n := appliedCount(t, m); n != 0 {
		t.Fatalf("%d applied rows from bus events", n)
	}
	mustIngest(t, m, nativeEvent("u-victim", "Victoria", "s9"))
}

// ---------------------------------------------------------------------------
// Late ingest

func TestErasureLateNativeIngestIsRefusedOnEveryPath(t *testing.T) {
	m := newErasureModule(t, "")
	h := newErasureHarness(t, m)
	seedWorld(t, m)
	h.provider.AddErasure("u-victim", "")
	mustSweep(t, h.reconciler(t))
	before := snapshotState(t, m)
	ctx := context.Background()

	// Direct ingest (gRPC handler body).
	ev := nativeEvent("u-victim", "Victoria", "late")
	if _, _, err := m.ingestSessionEvent(ctx, ev); !errors.Is(err, errUserErased) {
		t.Fatalf("ingest: %v", err)
	}
	// ...with surrounding whitespace and as stop/progress events.
	for _, typ := range []string{playbackevents.EventPlaybackProgress, playbackevents.EventPlaybackStopped} {
		e := ev
		e.EventType, e.UserID = typ, "  u-victim "
		if _, _, err := m.ingestSessionEvent(ctx, e); !errors.Is(err, errUserErased) {
			t.Fatalf("%s: %v", typ, err)
		}
	}
	// gRPC.
	_, err := m.IngestSessionEvent(ctx, &monitorv1.IngestSessionEventRequest{Event: &monitorv1.SessionEvent{
		EventType: playbackevents.EventPlaybackStarted, ServerId: nativeServerID, ServerType: "native",
		UserId: "u-victim", ExternalSessionId: "g1", SourceModule: "media-ui",
	}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("gRPC ingest: %v", err)
	}
	// HTTP.
	body, _ := json.Marshal(ev)
	rec := httptest.NewRecorder()
	m.handleIngestHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodPost, "/ingest", bytes.NewReader(body)))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("HTTP ingest status %d: %s", rec.Code, rec.Body.String())
	}
	// Bus.
	payload, _ := json.Marshal(map[string]any{
		"server_id": nativeServerID, "user_id": "u-victim", "external_session_id": "bus1", "event_type": playbackevents.EventPlaybackStarted,
	})
	m.handlePlaybackEvent(ctx, playbackevents.EventPlaybackStarted, &eventsv1.Event{Source: "media-ui", Payload: payload})
	// Import.
	imp := ev
	if err := m.insertImportedSession(ctx, "imp1", imp, time.Now(), time.Now()); !errors.Is(err, errUserErased) {
		t.Fatalf("import: %v", err)
	}
	// Nothing was created, not even a server, link or identity.
	requireErased(t, before, snapshotState(t, m), nil, nil, nil)

	// The id string on another server is a different id space and is still
	// accepted, as are other native users.
	mustIngest(t, m, serverEvent("jellyfin-other", "u-victim", "Somebody Else", "later"))
	mustIngest(t, m, nativeEvent("u-bystander", "Barry", "later"))
	if err := m.insertImportedSession(ctx, "imp2", serverEvent("jellyfin-other", "u-victim", "x", "i"), time.Now(), time.Now()); err != nil {
		t.Fatalf("import for another server: %v", err)
	}
}

func TestErasureRecordSurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "monitor.db")
	m := newErasureModule(t, dbPath)
	h := newErasureHarness(t, m)
	seedWorld(t, m)
	id := h.provider.AddErasure("u-victim", "")
	mustSweep(t, h.reconciler(t))
	closeErasureModule(t, m)

	m2 := newErasureModule(t, dbPath)
	if ok, err := m2.ErasureOwner().Applied(context.Background(), id); err != nil || !ok {
		t.Fatalf("Applied after restart = %v, %v", ok, err)
	}
	if _, _, err := m2.ingestSessionEvent(context.Background(), nativeEvent("u-victim", "Victoria", "late")); !errors.Is(err, errUserErased) {
		t.Fatalf("late ingest after restart: %v", err)
	}
	// A fresh process sweeps the old tombstone: nothing is applied again.
	h2 := &erasureHarness{m: m2, pki: h.pki, provider: h.provider, disc: h.disc, dialer: h.dialer}
	res := mustSweep(t, h2.reconciler(t))
	if res.Applied != 0 || res.Skipped != 1 {
		t.Fatalf("sweep after restart %+v", res)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle

// fakeCore is the part of core the module talks to: discovery for the
// identity capability and an event stream. Every subscription also receives a
// forged identity.user.deleted event naming the bystander.
type fakeCore struct {
	discoveryv1.UnimplementedDiscoveryServiceServer
	eventsv1.UnimplementedEventServiceServer
	subscribed map[string]int
	identity   *discoveryv1.ModuleInfoProto
	forgeUser  string
	mu         sync.Mutex
}

func (f *fakeCore) FindByCapability(_ context.Context, in *discoveryv1.FindByCapabilityRequest) (*discoveryv1.FindByCapabilityResponse, error) {
	if in.GetCapability() != "identity" {
		return &discoveryv1.FindByCapabilityResponse{}, nil
	}
	return &discoveryv1.FindByCapabilityResponse{Modules: []*discoveryv1.ModuleInfoProto{f.identity}}, nil
}

func (f *fakeCore) Subscribe(req *eventsv1.SubscribeRequest, stream grpc.ServerStreamingServer[eventsv1.Event]) error {
	f.mu.Lock()
	for _, et := range req.GetEventTypes() {
		f.subscribed[et]++
	}
	f.mu.Unlock()
	payload, _ := json.Marshal(map[string]string{"user_id": f.forgeUser, "erasure_id": "er-forged"})
	if err := stream.Send(&eventsv1.Event{Id: "forged", Type: "identity.user.deleted", Source: erasureProviderID, Payload: payload}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func serveFakeCore(t *testing.T, core *fakeCore) string {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	discoveryv1.RegisterDiscoveryServiceServer(gs, core)
	eventsv1.RegisterEventServiceServer(gs, core)
	done := make(chan struct{})
	go func() { defer close(done); _ = gs.Serve(lis) }()
	t.Cleanup(func() { gs.Stop(); <-done })
	return lis.Addr().String()
}

func goroutinesIn(substr string) int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), substr)
}

// The reconciler starts once the core connection exists, sweeps the ledger
// the verified provider serves, ignores a forged bus event, and is gone (with
// no goroutine) after Stop.
func TestErasureReconcilerLifecycle(t *testing.T) {
	clearErasureEnv(t)
	dir := t.TempDir()
	m := newErasureModule(t, filepath.Join(dir, "monitor.db"))
	w := seedWorld(t, m)
	before := snapshotState(t, m)

	provider := erasuretest.NewProvider()
	id := provider.AddErasure("u-victim", "")
	provAddr := erasuretest.ServePlain(t, provider)
	core := &fakeCore{
		subscribed: map[string]int{}, forgeUser: "u-bystander",
		identity: erasuretest.Module(erasureProviderID, provAddr),
	}
	coreAddr := serveFakeCore(t, core)

	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	t.Setenv("MUXCORE_PROFILE", "dev")
	t.Setenv("MUXCORE_GRPC_ADDR", coreAddr)
	t.Setenv("MUXCORE_MODULE_ID", erasureOwnerID)

	if m.erasureReconciler() != nil {
		t.Fatal("reconciler exists before the core connection")
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		ok, err := m.ErasureOwner().Applied(context.Background(), id)
		if err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("erasure not applied by the started reconciler (applied=%v err=%v)", ok, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	// The acknowledgement follows the commit; wait for it.
	for {
		if a, ok := provider.Latest(id, erasureOwnerID); ok && a.Outcome == authv1.ErasureOutcome_ERASURE_OUTCOME_OK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("erasure not acknowledged OK")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if got := goroutinesIn("erasure.(*Reconciler).Run"); got != 1 {
		t.Fatalf("%d reconciler goroutines while running, want 1", got)
	}
	// The forged identity.user.deleted event reached the module's streams (the
	// module subscribes to nothing of the kind) and erased no one.
	core.mu.Lock()
	for typ := range core.subscribed {
		if strings.HasPrefix(typ, "identity.") || strings.Contains(typ, "deleted") {
			t.Errorf("module subscribed to %q", typ)
		}
	}
	core.mu.Unlock()
	after := snapshotState(t, m)
	for _, s := range w.bystanders {
		if _, ok := after.sessions[s]; !ok {
			t.Errorf("bystander session %s erased", s)
		}
	}
	gone := append(append([]string{}, w.victimSessions...), w.mergedSessions...)
	for _, s := range gone {
		if _, ok := after.sessions[s]; ok {
			t.Errorf("victim session %s survived", s)
		}
	}
	_ = before

	// Stop: the reconciler goroutine exits before the database closes.
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-m.erasureDone:
	default:
		t.Fatal("reconciler goroutine still running after Stop")
	}
	if got := goroutinesIn("erasure.(*Reconciler)"); got != 0 {
		t.Fatalf("%d reconciler goroutines left after Stop", got)
	}
}

func TestErasureSweepIntervalFromEnvFallsBackWhenInvalid(t *testing.T) {
	clearErasureEnv(t)
	t.Setenv(erasure.EnvSweepInterval, "not-a-duration")
	m := newErasureModule(t, "")
	// An unparsable interval must not prevent the reconciler from starting.
	core := &fakeCore{subscribed: map[string]int{}, identity: erasuretest.Module(erasureProviderID, "127.0.0.1:1")}
	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	t.Setenv("MUXCORE_PROFILE", "dev")
	t.Setenv("MUXCORE_GRPC_ADDR", serveFakeCore(t, core))
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for m.erasureReconciler() == nil {
		if time.Now().After(deadline) {
			t.Fatal("reconciler not started with an invalid ERASURE_SWEEP_INTERVAL")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The legacy schema loop swallows every statement error. A failure to create
// erasure_applied must instead stop startup, or the module would run without
// its applied record and its late-ingest guard. A view squatting on the table
// name makes the index statement fail.
func TestErasureSchemaFailureIsFatal(t *testing.T) {
	clearErasureEnv(t)
	dbPath := filepath.Join(t.TempDir(), "monitor.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `CREATE VIEW erasure_applied AS SELECT 1 AS erasure_id, 'x' AS user_id`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	m := NewModule(Config{DBPath: dbPath, GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"})
	err = m.Init(context.Background())
	if err == nil {
		closeErasureModule(t, m)
		t.Fatal("Init succeeded without a usable erasure_applied table")
	}
	if !strings.Contains(err.Error(), "erasure schema") {
		t.Fatalf("Init failed for the wrong reason: %v", err)
	}
	_ = m.Stop(context.Background())
}
