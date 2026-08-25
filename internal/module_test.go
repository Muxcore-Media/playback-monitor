package internal

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func TestPlaybackMonitorIngestLifecycle(t *testing.T) {
	dir := t.TempDir()
	m := NewModule(Config{
		DBPath:   filepath.Join(dir, "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	ev := SessionEvent{
		EventType:         "playback.started",
		SourceModule:      "jellyfin",
		ServerType:        "jellyfin",
		ExternalSessionID: "sess-1",
		UserID:            "alice",
		UserName:          "Alice",
		ItemID:            "item-1",
		Title:             "Test Movie",
		PositionSeconds:   10,
		DurationSeconds:   6000,
	}
	id, created, err := m.ingestSessionEvent(ctx, ev)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if !created || id == "" {
		t.Fatalf("expected created session, got id=%q created=%v", id, created)
	}

	ev.EventType = "playback.progress"
	ev.PositionSeconds = 120
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatalf("progress: %v", err)
	}

	active, err := m.listActiveSessions(ctx, "", 10)
	if err != nil {
		t.Fatalf("active: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 active session, got %d", len(active))
	}
	if active[0].PositionSeconds != 120 {
		t.Fatalf("expected position 120, got %v", active[0].PositionSeconds)
	}

	ev.EventType = "playback.stopped"
	ev.PositionSeconds = 500
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatalf("stop: %v", err)
	}

	active, err = m.listActiveSessions(ctx, "", 10)
	if err != nil {
		t.Fatalf("active after stop: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected 0 active sessions, got %d", len(active))
	}

	history, total, err := m.listHistory(ctx, "", "", "", 10, 0)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if total != 1 || len(history) != 1 {
		t.Fatalf("expected 1 history row, got total=%d len=%d", total, len(history))
	}
	if history[0].State != "stopped" {
		t.Fatalf("expected stopped state, got %q", history[0].State)
	}

	stats, err := m.homeStats(ctx, 30)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if len(stats) == 0 {
		t.Fatal("expected home stats")
	}
}

func TestStreamAnalyticsAndUserStats(t *testing.T) {
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

	ev := SessionEvent{
		EventType:         "playback.started",
		ServerType:        "jellyfin",
		ServerID:          "jf1",
		ExternalSessionID: "sess-analytics",
		UserID:            "u1",
		UserName:          "alice",
		ItemID:            "item1",
		Title:             "Movie A",
		Platform:          "Android",
		IsTranscode:       true,
		PositionSeconds:   0,
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatalf("start: %v", err)
	}
	ev.EventType = "playback.stopped"
	ev.PositionSeconds = 600
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatalf("stop: %v", err)
	}

	platforms, transcodes, err := m.streamAnalytics(ctx, 30)
	if err != nil {
		t.Fatalf("analytics: %v", err)
	}
	if len(platforms) != 1 || platforms[0].Key != "Android" {
		t.Fatalf("platforms: %+v", platforms)
	}
	if len(transcodes) != 1 || transcodes[0].Key != "transcode" {
		t.Fatalf("transcodes: %+v", transcodes)
	}

	users, err := m.listUserWatchStats(ctx, 30, 10)
	if err != nil {
		t.Fatalf("users: %v", err)
	}
	if len(users) != 1 || users[0].Username != "alice" || users[0].PlayCount != 1 {
		t.Fatalf("users: %+v", users)
	}
}

func TestServerRegistryOnIngest(t *testing.T) {
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

	ev := SessionEvent{
		EventType:         "playback.started",
		SourceModule:      "plex",
		ServerType:        "plex",
		ExternalSessionID: "p1",
		UserName:          "alice",
		ItemID:            "rk1",
		Title:             "Movie",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	servers, err := m.listServers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range servers {
		if s.ID == "plex" && s.Type == "plex" {
			found = true
		}
	}
	if !found {
		t.Fatalf("servers: %+v", servers)
	}
}

func TestPlaysByDate(t *testing.T) {
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

	ev := SessionEvent{
		EventType:         "playback.started",
		SourceModule:      "jellyfin",
		ExternalSessionID: "s1",
		UserName:          "bob",
		ItemID:            "i1",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	ev.EventType = "playback.stopped"
	ev.PositionSeconds = 100
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	rows, err := m.playsByDate(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Count != 1 {
		t.Fatalf("rows: %+v", rows)
	}
}

func TestSessionEventFromPlaybackJSON(t *testing.T) {
	payload := []byte(`{
		"session_id":"abc",
		"user_name":"Bob",
		"jellyfin_item_id":"jf-1",
		"muxcore_id":"mx-1",
		"title":"Episode 1",
		"position_seconds":42,
		"duration_seconds":3600,
		"is_paused":true
	}`)
	ev, err := sessionEventFromPlaybackJSON("jellyfin", payload)
	if err != nil {
		t.Fatal(err)
	}
	if ev.ExternalSessionID != "abc" || ev.UserName != "Bob" || ev.ItemID != "jf-1" {
		t.Fatalf("unexpected ev: %+v", ev)
	}
	if !ev.IsPaused || ev.PositionSeconds != 42 {
		t.Fatalf("pause/position mismatch: %+v", ev)
	}
}

func TestItemWatchStatsAggregation(t *testing.T) {
	dir := t.TempDir()
	m := NewModule(Config{
		DBPath:   filepath.Join(dir, "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	itemID := "movie-abc"
	for i, user := range []string{"alice", "bob"} {
		ev := SessionEvent{
			EventType:         "playback.started",
			SourceModule:      "jellyfin",
			ExternalSessionID: fmt.Sprintf("sess-%d", i),
			UserName:          user,
			ItemID:            itemID,
			MuxcoreID:         itemID,
			Title:             "Test",
			DurationSeconds:   3600,
		}
		if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
			t.Fatalf("start: %v", err)
		}
		ev.EventType = "playback.stopped"
		ev.PositionSeconds = 1800
		if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
			t.Fatalf("stop: %v", err)
		}
	}

	ws, err := m.itemWatchStats(ctx, itemID, 60)
	if err != nil {
		t.Fatal(err)
	}
	if ws.UniqueUsers != 2 || ws.PlayCount != 2 || !ws.HasActivity {
		t.Fatalf("unexpected stats: %+v", ws)
	}
	if ws.TotalDurationMinutes != 60 {
		t.Fatalf("total minutes %v", ws.TotalDurationMinutes)
	}

	users, err := m.listWatchUsers(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 {
		t.Fatalf("users %v", users)
	}
}
