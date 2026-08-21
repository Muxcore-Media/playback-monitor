package internal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestApplyLibraryCatalogEvent(t *testing.T) {
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

	ev := libraryCatalogEvent{
		Action: "added", ServerID: "jf1", ServerType: "jellyfin",
		ItemID: "movie-1", Title: "Test Movie", MediaType: "Movie",
		FileSizeBytes: 12345, LibraryName: "Movies",
	}
	if err := m.applyLibraryCatalogEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	rec, err := m.getLibraryItem(ctx, "jf1", "movie-1")
	if err != nil || rec == nil {
		t.Fatalf("get: %v", err)
	}
	if rec.Title != "Test Movie" {
		t.Fatalf("title: %q", rec.Title)
	}
	if rec.FileSizeBytes != 12345 {
		t.Fatalf("size: %d", rec.FileSizeBytes)
	}

	ev.Action = "removed"
	if err := m.applyLibraryCatalogEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	rec, err = m.getLibraryItem(ctx, "jf1", "movie-1")
	if err != nil || rec == nil {
		t.Fatalf("expected soft-removed row, got %v err=%v", rec, err)
	}
}

func TestLibraryCatalogFromJSON(t *testing.T) {
	ev, err := libraryCatalogFromJSON("jellyfin", []byte(`{
		"action":"added","server_id":"s1","item_id":"i1","media_type":"Episode"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.ServerID != "s1" || ev.ItemID != "i1" || ev.MediaType != "Episode" {
		t.Fatalf("%+v", ev)
	}
}
