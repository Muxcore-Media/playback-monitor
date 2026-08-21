package internal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestPlaysBySourceAndPlatformResolution(t *testing.T) {
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
		ServerID: "s1", ItemID: "m1", Title: "Movie", VideoResolution: "1080",
	}); err != nil {
		t.Fatal(err)
	}

	start := SessionEvent{
		EventType: "playback.started", ServerID: "s1", ExternalSessionID: "a1",
		UserID: "u1", ItemID: "m1", Platform: "iOS", StreamResolution: "720",
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

	source, err := m.playsBySourceResolution(ctx, 30, 10)
	if err != nil || len(source) != 1 || source[0].Label != "1080" {
		t.Fatalf("source resolution: %+v err=%v", source, err)
	}

	combo, err := m.playsByPlatformResolution(ctx, 30, 10)
	if err != nil || len(combo) != 1 || combo[0].Label != "iOS · 720" {
		t.Fatalf("platform resolution: %+v err=%v", combo, err)
	}
}
