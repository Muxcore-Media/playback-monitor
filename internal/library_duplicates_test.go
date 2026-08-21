package internal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestListLibraryDuplicates(t *testing.T) {
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

	items := []libraryCatalogEvent{
		{Action: "added", ServerID: "s1", ServerType: "jellyfin", ItemID: "a1", MuxcoreID: "mc-dup", Title: "Inception"},
		{Action: "added", ServerID: "s2", ServerType: "jellyfin", ItemID: "a2", MuxcoreID: "mc-dup", Title: "Inception"},
		{Action: "added", ServerID: "s1", ServerType: "jellyfin", ItemID: "b1", Title: "Unique Movie"},
	}
	for _, item := range items {
		if err := m.applyLibraryCatalogEvent(ctx, item); err != nil {
			t.Fatal(err)
		}
	}

	groups, err := m.listLibraryDuplicates(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].CopyCount != 2 {
		t.Fatalf("groups=%+v", groups)
	}
}
