package internal

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestListRecentlyAddedLibraryItems(t *testing.T) {
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

	older := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
	newer := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)

	for _, row := range []struct {
		itemID, title, added string
	}{
		{"old-movie", "Old Movie", older},
		{"new-movie", "New Movie", newer},
	} {
		if _, err := m.exec(ctx, `
			INSERT INTO library_items(
				server_id, item_id, title, media_type, library_name, catalog_added_at, updated_at
			) VALUES ('s1', ?, ?, 'Movie', 'Movies', ?, ?)`,
			row.itemID, row.title, row.added, row.added,
		); err != nil {
			t.Fatal(err)
		}
	}

	items, cursor, err := m.listRecentlyAddedLibraryItems(ctx, "s1", "Movies", "movie", false, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ItemID != "new-movie" {
		t.Fatalf("page1: %+v", items)
	}
	if cursor == "" {
		t.Fatal("expected cursor")
	}

	items2, _, err := m.listRecentlyAddedLibraryItems(ctx, "s1", "Movies", "movie", false, 1, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(items2) != 1 || items2[0].ItemID != "old-movie" {
		t.Fatalf("page2: %+v", items2)
	}
}

func TestRecentlyAddedExcludesRemovedByDefault(t *testing.T) {
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

	now := time.Now().UTC().Format(time.RFC3339)
	if err := m.applyLibraryCatalogEvent(ctx, libraryCatalogEvent{
		Action: "added", ServerID: "s1", ServerType: "jellyfin",
		ItemID: "gone", Title: "Removed", MediaType: "Movie", LibraryName: "Movies",
	}); err != nil {
		t.Fatal(err)
	}
	ev := libraryCatalogEvent{Action: "removed", ServerID: "s1", ItemID: "gone"}
	if err := m.applyLibraryCatalogEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	_ = now

	items, _, err := m.listRecentlyAddedLibraryItems(ctx, "", "", "", false, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.ItemID == "gone" {
			t.Fatalf("removed item included: %+v", items)
		}
	}

	items, _, err = m.listRecentlyAddedLibraryItems(ctx, "", "", "", true, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range items {
		if item.ItemID == "gone" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected removed item when include_removed=true: %+v", items)
	}
}
