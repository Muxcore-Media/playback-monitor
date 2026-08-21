package internal

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestListStaleLibraryItems(t *testing.T) {
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

	old := time.Now().UTC().AddDate(0, 0, -120)
	if err := m.applyLibraryCatalogEvent(ctx, libraryCatalogEvent{
		Action: "added", ServerID: "s1", ServerType: "jellyfin",
		ItemID: "never-1", Title: "Never Seen",
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.applyLibraryCatalogEvent(ctx, libraryCatalogEvent{
		Action: "added", ServerID: "s1", ServerType: "jellyfin",
		ItemID: "old-1", Title: "Old Hit",
	}); err != nil {
		t.Fatal(err)
	}

	start := SessionEvent{
		EventType: "playback.started", ServerID: "s1", ExternalSessionID: "stale-1",
		ItemID: "old-1", OccurredAt: old,
	}
	if _, _, err := m.ingestSessionEvent(ctx, start); err != nil {
		t.Fatal(err)
	}
	stop := start
	stop.EventType = "playback.stopped"
	stop.OccurredAt = old.Add(time.Hour)
	if _, _, err := m.ingestSessionEvent(ctx, stop); err != nil {
		t.Fatal(err)
	}

	items, never, stale := 0, 0, 0
	var err error
	var list []staleLibraryItem
	list, never, stale, err = m.listStaleLibraryItems(ctx, "s1", 90, 20)
	if err != nil {
		t.Fatal(err)
	}
	items = len(list)
	if items < 2 || never < 1 || stale < 1 {
		t.Fatalf("items=%d never=%d stale=%d list=%+v", items, never, stale, list)
	}
}
