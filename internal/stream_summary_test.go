package internal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestBuildStreamSummaryPlayCategories(t *testing.T) {
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

	rows := []SessionRecord{
		{ServerID: "default", IsTranscode: true, PlayMethod: "Transcode"},
		{ServerID: "default", PlayMethod: "DirectStream"},
		{ServerID: "default", PlayMethod: "DirectPlay"},
	}
	summary := m.buildStreamSummary(ctx, rows)
	if summary["total"].(int) != 3 {
		t.Fatalf("total: %#v", summary["total"])
	}
	if summary["transcodes"].(int) != 1 {
		t.Fatalf("transcodes: %#v", summary["transcodes"])
	}
	if summary["direct_streams"].(int) != 1 {
		t.Fatalf("direct_streams: %#v", summary["direct_streams"])
	}
	if summary["direct_plays"].(int) != 1 {
		t.Fatalf("direct_plays: %#v", summary["direct_plays"])
	}
	byServer, ok := summary["by_server"].([]map[string]any)
	if !ok || len(byServer) != 1 {
		t.Fatalf("by_server: %#v", summary["by_server"])
	}
	row := byServer[0]
	if row["total"].(int) != 3 || row["transcodes"].(int) != 1 || row["direct_streams"].(int) != 1 || row["direct_plays"].(int) != 1 {
		t.Fatalf("server row: %#v", row)
	}
	if row["server_name"].(string) != "Default Server" {
		t.Fatalf("server_name: %#v", row["server_name"])
	}
}

func TestIngestStoresPlayMethod(t *testing.T) {
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
		ExternalSessionID: "play-method",
		UserName:          "alice",
		ItemID:            "item-1",
		PlayMethod:        "DirectStream",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	active, err := m.listActiveSessions(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].PlayMethod != "DirectStream" {
		t.Fatalf("active: %#v", active)
	}
}
