package internal

import (
	"context"
	"path/filepath"
	"testing"

	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

const sampleJellystatBackup = `[{
  "jf_playback_activity": [
    {
      "Id": "js-1001",
      "UserId": "user-abc",
      "UserName": "Bob",
      "NowPlayingItemId": "item-1",
      "NowPlayingItemName": "Interstellar",
      "PlaybackDuration": "3600",
      "ActivityDateInserted": "2024-12-13T15:00:00.000Z",
      "PlayMethod": "DirectStream",
      "Client": "Jellyfin Web",
      "DeviceName": "Chrome",
      "RemoteEndPoint": "203.0.113.5"
    },
    {
      "Id": "js-1001",
      "UserId": "user-abc",
      "NowPlayingItemName": "Duplicate",
      "PlaybackDuration": "3600",
      "ActivityDateInserted": "2024-12-13T15:00:00.000Z"
    },
    {
      "Id": "js-1002",
      "UserId": "user-abc",
      "NowPlayingItemName": "Pilot",
      "SeriesName": "Code Black",
      "PlaybackDuration": "1800",
      "ActivityDateInserted": "2025-04-05T10:40:28.000Z",
      "PlayMethod": "Transcode"
    }
  ]
}]`

func TestImportJellystatHistory(t *testing.T) {
	ctx := context.Background()
	m := NewModule(Config{
		DBPath:   filepath.Join(t.TempDir(), "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	resp, err := m.ImportJellystatHistory(ctx, &monitorv1.ImportJellystatHistoryRequest{
		ServerId:   "jf-home",
		BackupJson: sampleJellystatBackup,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetError() != "" {
		t.Fatalf("unexpected error: %s", resp.GetError())
	}
	if resp.GetImported() != 2 {
		t.Fatalf("expected 2 imported, got %d (skipped=%d failed=%d)", resp.GetImported(), resp.GetSkipped(), resp.GetFailed())
	}
	if resp.GetSkipped() != 1 {
		t.Fatalf("expected 1 skipped duplicate, got %d", resp.GetSkipped())
	}

	history, total, err := m.listHistory(ctx, "jf-home", "", "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(history) != 2 {
		t.Fatalf("history total=%d len=%d", total, len(history))
	}
}

func TestParseJellystatBackupJSON(t *testing.T) {
	records, err := parseJellystatBackupJSON(sampleJellystatBackup)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 activities, got %d", len(records))
	}
}

func TestJellystatRecordToSessionEvent(t *testing.T) {
	records, err := parseJellystatBackupJSON(sampleJellystatBackup)
	if err != nil {
		t.Fatal(err)
	}
	_, ev, _, _, ok := jellystatRecordToSessionEvent(records[0], "jf", "jellyfin")
	if !ok {
		t.Fatal("expected ok")
	}
	if ev.Title != "Interstellar" || ev.MediaType != "movie" || ev.IsTranscode {
		t.Fatalf("unexpected ev: %+v", ev)
	}
	_, ev2, _, _, ok := jellystatRecordToSessionEvent(records[2], "jf", "jellyfin")
	if !ok || ev2.MediaType != "episode" || !ev2.IsTranscode {
		t.Fatalf("episode ev: %+v ok=%v", ev2, ok)
	}
}
