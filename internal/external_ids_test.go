package internal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestExternalIDHistoryFilter(t *testing.T) {
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
		ServerID: "s1", ServerType: "jellyfin", ItemID: "m1", Title: "Movie",
		MediaType: "Movie", ImdbID: "tt1234567", TmdbID: 42,
	}); err != nil {
		t.Fatal(err)
	}

	ev := SessionEvent{
		EventType: "playback.started", ServerID: "s1", ExternalSessionID: "x1",
		UserID: "u1", UserName: "alice", ItemID: "m1", Title: "Movie",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	ev.EventType = "playback.stopped"
	ev.PositionSeconds = 90
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	rows, _, err := m.listHistoryPageFiltered(ctx, historyPageFilter{ImdbID: "tt1234567"}, 25, "")
	if err != nil || len(rows) != 1 {
		t.Fatalf("imdb filter: rows=%+v err=%v", rows, err)
	}
	if rows[0].ImdbID != "tt1234567" {
		t.Fatalf("session imdb: %q", rows[0].ImdbID)
	}

	rows, _, err = m.listHistoryPageFiltered(ctx, historyPageFilter{TmdbID: 42}, 25, "")
	if err != nil || len(rows) != 1 {
		t.Fatalf("tmdb filter: rows=%+v err=%v", rows, err)
	}
}

func TestPerLibraryStorageHistory(t *testing.T) {
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
		ServerID: "s1", ServerType: "jellyfin", ItemID: "m1", LibraryName: "Movies", FileSizeBytes: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.upsertLibraryCatalogItem(ctx, libraryCatalogEvent{
		ServerID: "s1", ServerType: "jellyfin", ItemID: "e1", LibraryName: "TV", FileSizeBytes: 500,
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.snapshotLibraryStorageToday(ctx, ""); err != nil {
		t.Fatal(err)
	}

	movies, err := m.getLibraryStorageHistory(ctx, "", "Movies", 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(movies) != 1 || movies[0].TotalBytes != 1000 {
		t.Fatalf("Movies history: %+v", movies)
	}
	tv, err := m.getLibraryStorageHistory(ctx, "", "TV", 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(tv) != 1 || tv[0].TotalBytes != 500 {
		t.Fatalf("TV history: %+v", tv)
	}
}
