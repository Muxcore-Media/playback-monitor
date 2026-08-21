package internal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestGetLibraryStorageSummary(t *testing.T) {
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

	if err := m.applyLibraryCatalogEvent(ctx, libraryCatalogEvent{
		Action: "added", ServerID: "s1", ServerType: "jellyfin",
		ItemID: "a1", Title: "Movie A", MuxcoreID: "mc-a", LibraryName: "Movies", FileSizeBytes: 1_000_000_000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.applyLibraryCatalogEvent(ctx, libraryCatalogEvent{
		Action: "added", ServerID: "s2", ServerType: "plex",
		ItemID: "a2", Title: "Movie A (remux)", MuxcoreID: "mc-a", LibraryName: "Movies", FileSizeBytes: 1_000_000_000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.applyLibraryCatalogEvent(ctx, libraryCatalogEvent{
		Action: "added", ServerID: "s2", ServerType: "plex",
		ItemID: "b1", Title: "Show B", LibraryName: "TV", FileSizeBytes: 500_000_000,
	}); err != nil {
		t.Fatal(err)
	}

	summary, err := m.getLibraryStorageSummary(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if summary.TotalItems != 3 {
		t.Fatalf("items: %d", summary.TotalItems)
	}
	if summary.TotalBytes != 2_500_000_000 {
		t.Fatalf("bytes: %d", summary.TotalBytes)
	}
	if summary.DuplicateWaste != 1_000_000_000 {
		t.Fatalf("waste: %d", summary.DuplicateWaste)
	}
	if len(summary.Libraries) != 3 {
		t.Fatalf("libraries: %d", len(summary.Libraries))
	}
}

func TestFormatBytes(t *testing.T) {
	if formatBytes(0) != "0 B" {
		t.Fatalf("zero: %q", formatBytes(0))
	}
	if formatBytes(1536) != "1.5 KB" {
		t.Fatalf("kb: %q", formatBytes(1536))
	}
}
