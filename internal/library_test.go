package internal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestLibraryStatsAndTopContent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := NewModule(Config{
		DBPath:   filepath.Join(dir, "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	start := SessionEvent{
		EventType:         "playback.started",
		SourceModule:      "jellyfin",
		ServerID:          "jf1",
		ExternalSessionID: "lib-1",
		UserName:          "alice",
		ItemID:            "m1",
		Title:             "Inception",
		MediaType:         "Movie",
		MediaPath:         "/media/movies/Inception.mkv",
	}
	if _, _, err := m.ingestSessionEvent(ctx, start); err != nil {
		t.Fatal(err)
	}
	stop := start
	stop.EventType = "playback.stopped"
	stop.PositionSeconds = 7200
	if _, _, err := m.ingestSessionEvent(ctx, stop); err != nil {
		t.Fatal(err)
	}

	epStart := SessionEvent{
		EventType:         "playback.started",
		SourceModule:      "jellyfin",
		ServerID:          "jf1",
		ExternalSessionID: "lib-2",
		UserName:          "bob",
		ItemID:            "e1",
		Title:             "Pilot",
		MediaType:         "Episode",
		MediaPath:         "/data/tv/Show/Season 1/Pilot.mkv",
	}
	if _, _, err := m.ingestSessionEvent(ctx, epStart); err != nil {
		t.Fatal(err)
	}
	epStop := epStart
	epStop.EventType = "playback.stopped"
	epStop.PositionSeconds = 1800
	if _, _, err := m.ingestSessionEvent(ctx, epStop); err != nil {
		t.Fatal(err)
	}

	libs, err := m.listLibraryStats(ctx, 30, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(libs) != 2 {
		t.Fatalf("libraries: %+v", libs)
	}
	foundMovies := false
	foundTV := false
	for _, row := range libs {
		switch row.LibraryName {
		case "movies":
			foundMovies = true
			if row.PlayCount != 1 {
				t.Fatalf("movies play count: %+v", row)
			}
		case "tv":
			foundTV = true
		}
	}
	if !foundMovies || !foundTV {
		t.Fatalf("expected movies and tv libraries, got %+v", libs)
	}

	movies, shows, _, err := m.listTopContent(ctx, 30, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(movies) != 1 || movies[0].Title != "Inception" {
		t.Fatalf("movies: %+v", movies)
	}
	if len(shows) != 1 || shows[0].Title != "Pilot" {
		t.Fatalf("shows: %+v", shows)
	}
}

func TestDeriveLibraryName(t *testing.T) {
	if got := deriveLibraryName("/media/movies/Film.mkv"); got != "movies" {
		t.Fatalf("got %q", got)
	}
	if got := deriveLibraryName("/data/tv/Show/file.mkv"); got != "tv" {
		t.Fatalf("got %q", got)
	}
	if got := deriveLibraryName(""); got != "" {
		t.Fatalf("got %q", got)
	}
}
