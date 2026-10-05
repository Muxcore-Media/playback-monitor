package internal

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Muxcore-Media/contracts-media/events"
)

func TestDeleteAccountPlayback(t *testing.T) {
	ctx := context.Background()
	m := NewModule(Config{
		DBPath:   filepath.Join(t.TempDir(), "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	for _, ev := range []SessionEvent{
		{EventType: "playback.started", ServerID: "jf1", ExternalSessionID: "s1", UserID: "acct", UserName: "Ada", ItemID: "i1"},
		{EventType: "playback.stopped", ServerID: "jf1", ExternalSessionID: "s1", UserID: "acct", UserName: "Ada", ItemID: "i1"},
		{EventType: "playback.started", ServerID: "jf1", ExternalSessionID: "s2", UserID: "other", UserName: "Bea", ItemID: "i2"},
		{EventType: "playback.stopped", ServerID: "jf1", ExternalSessionID: "s2", UserID: "other", UserName: "Bea", ItemID: "i2"},
	} {
		if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}

	n, err := m.DeleteAccountPlayback(ctx, "acct")
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("deleted %d sessions, want at least 1", n)
	}
	var left int
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM sessions WHERE user_id = ?`, "acct").Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("acct sessions left %d", left)
	}
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM sessions WHERE user_id = ?`, "other").Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 1 {
		t.Fatalf("other sessions %d, want 1", left)
	}
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM server_users WHERE external_user_id = ?`, "acct").Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("acct server_users left %d", left)
	}
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM server_users WHERE external_user_id = ?`, "other").Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 1 {
		t.Fatalf("other server_users %d, want 1", left)
	}

	n, err = m.DeleteAccountPlayback(ctx, "acct")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("second pass deleted %d", n)
	}
	if _, err := m.DeleteAccountPlayback(ctx, " "); err == nil {
		t.Fatal("expected blank user id to fail")
	}
}

func TestDeleteAccountPlaybackKeepsSharedIdentity(t *testing.T) {
	ctx := context.Background()
	m := NewModule(Config{
		DBPath:   filepath.Join(t.TempDir(), "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	ev := SessionEvent{
		EventType: "playback.stopped", ServerID: "jf1", ExternalSessionID: "s1",
		UserID: "acct", UserName: "Ada", ItemID: "i1",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	var identityID string
	if err := m.queryRow(ctx, `SELECT identity_id FROM server_users WHERE external_user_id = ?`, "acct").Scan(&identityID); err != nil {
		t.Fatal(err)
	}
	now := "2026-10-05T00:00:00Z"
	if _, err := m.exec(ctx,
		`INSERT INTO server_users(server_id, external_user_id, identity_id, user_name, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?)`,
		"jf2", "kept", identityID, "Kept", now, now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := m.DeleteAccountPlayback(ctx, "acct"); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM user_identities WHERE id = ?`, identityID).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 1 {
		t.Fatal("shared identity was deleted")
	}
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM server_users WHERE external_user_id = ?`, "kept").Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 1 {
		t.Fatal("kept server user was deleted")
	}
}

func TestApplyUserDeletedPlayback(t *testing.T) {
	ctx := context.Background()
	m := NewModule(Config{
		DBPath:   filepath.Join(t.TempDir(), "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })
	if _, _, err := m.ingestSessionEvent(ctx, SessionEvent{
		EventType: "playback.stopped", ServerID: "jf1", ExternalSessionID: "s1",
		UserID: "acct", UserName: "Ada", ItemID: "i1",
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(events.UserDeletedPayload{UserID: "acct"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ApplyUserDeleted(ctx, raw); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM sessions WHERE user_id = ?`, "acct").Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("sessions left %d", left)
	}
	if err := m.ApplyUserDeleted(ctx, []byte(`{}`)); err == nil {
		t.Fatal("expected missing user_id to fail")
	}
	if err := m.ApplyUserDeleted(ctx, []byte(`not-json`)); err == nil {
		t.Fatal("expected bad JSON to fail")
	}
}
