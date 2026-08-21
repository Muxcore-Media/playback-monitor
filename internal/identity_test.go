package internal

import (
	"context"
	"path/filepath"
	"testing"

	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
	"github.com/google/uuid"
)

func TestEnsureUserIdentityAndMerge(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := NewModule(Config{
		DBPath:   filepath.Join(dir, "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	ev := SessionEvent{
		EventType:         "playback.started",
		ServerID:          "jf1",
		ExternalSessionID: "s1",
		UserID:            "old",
		UserName:          "OldUser",
		ItemID:            "i1",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	ev.EventType = "playback.stopped"
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	idents, _, err := m.listPublicUserIdentities(ctx, 10, "")
	if err != nil || len(idents) != 1 {
		t.Fatalf("idents: %+v err=%v", idents, err)
	}
	if _, err := uuid.Parse(idents[0].ID); err != nil {
		t.Fatalf("expected UUID identity, got %q", idents[0].ID)
	}

	resp, err := m.MergeUserIdentity(ctx, &monitorv1.MergeUserIdentityRequest{
		SourceUserId: "old", TargetUserId: "new", TargetUserName: "NewUser",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSessionsUpdated() != 1 {
		t.Fatalf("updated: %d", resp.GetSessionsUpdated())
	}

	identsAfter, _, err := m.listPublicUserIdentities(ctx, 10, "")
	if err != nil || len(identsAfter) != 1 {
		t.Fatalf("idents after merge: %+v err=%v", identsAfter, err)
	}
	ident, err := m.getPublicUserIdentity(ctx, identsAfter[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := m.publicUserStats(ctx, ident.ID)
	if err != nil || stats["all_time"].Plays != 1 {
		t.Fatalf("stats: %+v err=%v", stats, err)
	}
}

func TestMergeUserIdentityRPC(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := NewModule(Config{
		DBPath:   filepath.Join(dir, "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	ev := SessionEvent{
		EventType:         "playback.started",
		ExternalSessionID: "s1",
		UserID:            "old",
		UserName:          "OldUser",
		ItemID:            "i1",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	ev.EventType = "playback.stopped"
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	resp, err := m.MergeUserIdentity(ctx, &monitorv1.MergeUserIdentityRequest{
		SourceUserId: "old", TargetUserId: "new", TargetUserName: "NewUser",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSessionsUpdated() != 1 {
		t.Fatalf("updated: %d", resp.GetSessionsUpdated())
	}
	history, total, err := m.listHistory(ctx, "", "new", "", 10, 0)
	if err != nil || total != 1 || len(history) != 1 || history[0].UserName != "NewUser" {
		t.Fatalf("history: total=%d len=%d row=%+v err=%v", total, len(history), history, err)
	}
}
