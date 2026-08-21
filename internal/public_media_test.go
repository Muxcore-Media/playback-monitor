package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestPublicMediaAPI(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	t.Setenv("PLAYBACK_MONITOR_PUBLIC_API_KEY", "test-public-key")
	m := NewModule(Config{
		DBPath:   filepath.Join(dir, "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	ev := SessionEvent{
		EventType:         "playback.started",
		SourceModule:      "jellyfin",
		ServerID:          "jf1",
		ServerType:        "jellyfin",
		ExternalSessionID: "m-s1",
		UserID:            "u1",
		UserName:          "Alice",
		ItemID:            "movie-42",
		MuxcoreID:         "mc-42",
		Title:             "Test Movie",
		MediaType:         "movie",
		LibraryName:       "Movies",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	ev.EventType = "playback.stopped"
	ev.PositionSeconds = 3600
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	mediaRec := httptest.NewRecorder()
	mediaReq := httptest.NewRequest(http.MethodGet, "/api/v2/public/media/jf1:movie-42", nil)
	mediaReq.Header.Set("Authorization", "Bearer test-public-key")
	mediaReq.SetPathValue("ref", "jf1:movie-42")
	m.handlePublicMedia(mediaRec, mediaReq)
	if mediaRec.Code != http.StatusOK {
		t.Fatalf("media: %d %s", mediaRec.Code, mediaRec.Body.String())
	}

	statsRec := httptest.NewRecorder()
	statsReq := httptest.NewRequest(http.MethodGet, "/api/v2/public/media/jf1:movie-42/stats", nil)
	statsReq.Header.Set("Authorization", "Bearer test-public-key")
	statsReq.SetPathValue("ref", "jf1:movie-42")
	m.handlePublicMediaStats(statsRec, statsReq)
	if statsRec.Code != http.StatusOK {
		t.Fatalf("stats: %d %s", statsRec.Code, statsRec.Body.String())
	}
	var statsBody map[string]any
	if err := json.Unmarshal(statsRec.Body.Bytes(), &statsBody); err != nil {
		t.Fatal(err)
	}
	windows, ok := statsBody["windows"].(map[string]any)
	if !ok {
		t.Fatalf("windows: %#v", statsBody)
	}
	allTime, ok := windows["all_time"].(map[string]any)
	if !ok || int(allTime["plays"].(float64)) != 1 {
		t.Fatalf("all_time: %#v", windows["all_time"])
	}

	watchRec := httptest.NewRecorder()
	watchReq := httptest.NewRequest(http.MethodGet, "/api/v2/public/media/muxcore:mc-42/watchers", nil)
	watchReq.Header.Set("Authorization", "Bearer test-public-key")
	watchReq.SetPathValue("ref", "muxcore:mc-42")
	m.handlePublicMediaWatchers(watchRec, watchReq)
	if watchRec.Code != http.StatusOK {
		t.Fatalf("watchers: %d %s", watchRec.Code, watchRec.Body.String())
	}
}
