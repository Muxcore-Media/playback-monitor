package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestPlaysByHourAndDayOfWeek(t *testing.T) {
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
		ExternalSessionID: "s-hour",
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

	hours, err := m.playsByHour(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(hours) != 24 {
		t.Fatalf("expected 24 hour buckets, got %d", len(hours))
	}
	totalHour := 0
	for _, row := range hours {
		totalHour += row.Count
	}
	if totalHour != 1 {
		t.Fatalf("expected 1 play in hour chart, got %d", totalHour)
	}

	dow, err := m.playsByDayOfWeek(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(dow) != 7 {
		t.Fatalf("expected 7 day buckets, got %d", len(dow))
	}
	totalDOW := 0
	for _, row := range dow {
		totalDOW += row.Count
	}
	if totalDOW != 1 {
		t.Fatalf("expected 1 play in dow chart, got %d", totalDOW)
	}
}

func TestPublicAPIAuthAndHistoryCursor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	t.Setenv("PLAYBACK_MONITOR_PUBLIC_API_KEY", "test-public-key")
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
		ServerID:          "jf1",
		ExternalSessionID: "pub-1",
		UserID:            "u1",
		UserName:          "alice",
		ItemID:            "item1",
		Title:             "Movie",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	ev.EventType = "playback.stopped"
	ev.PositionSeconds = 120
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	unauth := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v2/public/history", nil)
	m.handlePublicHistory(unauth, req)
	if unauth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", unauth.Code)
	}

	rec := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v2/public/history?pageSize=1", nil)
	req.Header.Set("Authorization", "Bearer test-public-key")
	m.handlePublicHistory(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("history: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	data, ok := body["data"].([]any)
	if !ok || len(data) != 1 {
		t.Fatalf("expected 1 history row, got %#v", body["data"])
	}
	meta, ok := body["meta"].(map[string]any)
	if !ok {
		t.Fatalf("meta: %#v", body["meta"])
	}
	if meta["nextCursor"] != nil {
		t.Fatalf("expected null nextCursor for single row page, got %#v", meta["nextCursor"])
	}

	streamRec := httptest.NewRecorder()
	streamReq := httptest.NewRequest(http.MethodGet, "/api/v2/public/streams", nil)
	streamReq.Header.Set("Authorization", "Bearer test-public-key")
	m.handlePublicStreams(streamRec, streamReq)
	if streamRec.Code != http.StatusOK {
		t.Fatalf("streams: %d %s", streamRec.Code, streamRec.Body.String())
	}
}

func TestPublicHealthAndServers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	t.Setenv("PLAYBACK_MONITOR_PUBLIC_API_KEY", "test-public-key")
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
		ServerID:          "jf1",
		ServerType:        "jellyfin",
		ExternalSessionID: "live-1",
		UserName:          "alice",
		ItemID:            "item1",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	auth := func(path string) map[string]any {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer test-public-key")
		return decodePublicJSON(t, rec, req, m)
	}

	health := auth("/api/v2/public/health")
	if health["status"] != "ok" {
		t.Fatalf("health status: %#v", health["status"])
	}
	servers, ok := health["servers"].([]any)
	if !ok || len(servers) == 0 {
		t.Fatalf("health servers: %#v", health["servers"])
	}

	list := auth("/api/v2/public/servers")
	data, ok := list["data"].([]any)
	if !ok || len(data) == 0 {
		t.Fatalf("servers data: %#v", list["data"])
	}
}

func decodePublicJSON(t *testing.T, rec *httptest.ResponseRecorder, req *http.Request, m *Module) map[string]any {
	t.Helper()
	switch req.URL.Path {
	case "/api/v2/public/health":
		m.handlePublicHealth(rec, req)
	case "/api/v2/public/servers":
		m.handlePublicServers(rec, req)
	default:
		t.Fatalf("unsupported path %s", req.URL.Path)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status=%d body=%s", req.URL.Path, rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestPublicAPIDisabledWithoutKey(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_ = os.Unsetenv("PLAYBACK_MONITOR_PUBLIC_API_KEY")
	m := NewModule(Config{
		DBPath:   filepath.Join(dir, "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v2/public/streams", nil)
	req.Header.Set("Authorization", "Bearer anything")
	m.handlePublicStreams(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when disabled, got %d", rec.Code)
	}
}
