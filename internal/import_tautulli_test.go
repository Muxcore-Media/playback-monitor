package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

const sampleTautulliJSON = `{
  "response": {
    "result": "success",
    "data": {
      "recordsFiltered": 2,
      "data": [
        {
          "reference_id": 1001,
          "row_id": 5001,
          "started": 1700000000,
          "stopped": 1700003600,
          "duration": 3600,
          "play_duration": 3200,
          "user_id": 2,
          "friendly_name": "Alice",
          "platform": "Chrome",
          "player": "Plex Web",
          "ip_address": "203.0.113.10",
          "media_type": "movie",
          "rating_key": 12345,
          "full_title": "Test Movie",
          "transcode_decision": "direct play",
          "location": "US"
        },
        {
          "reference_id": 1001,
          "row_id": 5001,
          "started": 1700000000,
          "stopped": 1700003600,
          "duration": 3600,
          "play_duration": 3200,
          "user_id": 2,
          "friendly_name": "Alice",
          "full_title": "Duplicate"
        }
      ]
    }
  }
}`

func TestImportTautulliHistoryFromJSON(t *testing.T) {
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

	resp, err := m.ImportTautulliHistory(ctx, &monitorv1.ImportTautulliHistoryRequest{
		ServerId:    "plex-main",
		RecordsJson: sampleTautulliJSON,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetError() != "" {
		t.Fatalf("unexpected error: %s", resp.GetError())
	}
	if resp.GetImported() != 1 {
		t.Fatalf("expected 1 imported, got imported=%d skipped=%d failed=%d", resp.GetImported(), resp.GetSkipped(), resp.GetFailed())
	}
	if resp.GetSkipped() != 1 {
		t.Fatalf("expected 1 skipped duplicate, got %d", resp.GetSkipped())
	}

	history, total, err := m.listHistory(ctx, "plex-main", "", "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(history) != 1 {
		t.Fatalf("history total=%d len=%d", total, len(history))
	}
	if history[0].UserName != "Alice" || history[0].Title != "Test Movie" {
		t.Fatalf("unexpected row: %+v", history[0])
	}
	if history[0].ExternalSessionID != "tautulli:ref:1001" {
		t.Fatalf("external id %q", history[0].ExternalSessionID)
	}
}

func TestImportTautulliDryRun(t *testing.T) {
	ctx := context.Background()
	m := NewModule(Config{DBPath: filepath.Join(t.TempDir(), "monitor.db"), GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	resp, err := m.ImportTautulliHistory(ctx, &monitorv1.ImportTautulliHistoryRequest{
		RecordsJson: sampleTautulliJSON,
		DryRun:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetImported() != 1 {
		t.Fatalf("dry run imported=%d skipped=%d", resp.GetImported(), resp.GetSkipped())
	}
	if resp.GetSkipped() != 1 {
		t.Fatalf("dry run skipped=%d", resp.GetSkipped())
	}
	_, total, err := m.listHistory(ctx, "tautulli", "", "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Fatalf("dry run should not persist, total=%d", total)
	}
}

func TestFetchTautulliHistoryPageRejectsMetadataURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "should not reach metadata server", http.StatusTeapot)
	}))
	t.Cleanup(srv.Close)

	_, _, err := fetchTautulliHistoryPage(context.Background(), "http://169.254.169.254/", "key", 0, 10)
	if err == nil {
		t.Fatal("expected metadata url to be refused before dial")
	}
}

func TestParseTautulliRecordsJSON(t *testing.T) {
	records, err := parseTautulliRecordsJSON(sampleTautulliJSON)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}
