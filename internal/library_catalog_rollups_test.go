package internal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestLibraryCatalogRollups(t *testing.T) {
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

	if err := m.upsertLibraryCatalogItem(ctx, libraryCatalogEvent{
		ServerID: "s1", ServerType: "jellyfin", ItemID: "m1", Title: "Movie A",
		MediaType: "Movie", LibraryName: "Movies", FileSizeBytes: 1000, VideoResolution: "1080",
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.upsertLibraryCatalogItem(ctx, libraryCatalogEvent{
		ServerID: "s1", ServerType: "jellyfin", ItemID: "m2", Title: "Movie B",
		MediaType: "Movie", LibraryName: "Movies", FileSizeBytes: 2000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.upsertLibraryCatalogItem(ctx, libraryCatalogEvent{
		ServerID: "s1", ServerType: "jellyfin", ItemID: "e1", Title: "Ep 1",
		MediaType: "Episode", LibraryName: "TV", FileSizeBytes: 500,
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.removeLibraryItem(ctx, "s1", "m2"); err != nil {
		t.Fatal(err)
	}

	start := SessionEvent{
		EventType: "playback.started", ServerID: "s1", ExternalSessionID: "s1",
		ItemID: "m1", StreamResolution: "720",
	}
	if _, _, err := m.ingestSessionEvent(ctx, start); err != nil {
		t.Fatal(err)
	}
	stop := start
	stop.EventType = "playback.stopped"
	stop.PositionSeconds = 60
	if _, _, err := m.ingestSessionEvent(ctx, stop); err != nil {
		t.Fatal(err)
	}

	rows, err := m.listLibraryCatalogRollups(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 libraries, got %+v", rows)
	}
	var movies libraryCatalogRollup
	for _, row := range rows {
		if row.LibraryName == "Movies" {
			movies = row
		}
	}
	if movies.ItemCount != 1 {
		t.Fatalf("Movies item_count=%d, want 1 (removed m2 excluded)", movies.ItemCount)
	}
	if movies.MovieCount != 1 || movies.TotalFileSize != 1000 {
		t.Fatalf("Movies rollup: %+v", movies)
	}
	if movies.Resolutions["1080"] != 1 {
		t.Fatalf("resolutions (catalog file resolution, not session): %+v", movies.Resolutions)
	}

	groups, err := m.listLibraryDuplicates(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range groups {
		for _, c := range g.Copies {
			if c.ItemID == "m2" {
				t.Fatalf("removed item in duplicates: %+v", g)
			}
		}
	}
}
