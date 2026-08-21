package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestLibraryItemsUpsertOnIngest(t *testing.T) {
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

	ev := SessionEvent{
		EventType:         "playback.started",
		SourceModule:      "jellyfin",
		ServerID:          "jf1",
		ExternalSessionID: "lib-1",
		ItemID:            "item-99",
		Title:             "Inception",
		MediaType:         "movie",
		MediaPath:         "/media/movies/Inception.mkv",
		LibraryName:       "Movies",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	count, err := m.countLibraryItems(ctx, "jf1")
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	item, err := m.getLibraryItem(ctx, "jf1", "item-99")
	if err != nil || item == nil || item.Title != "Inception" || item.LibraryName != "Movies" {
		t.Fatalf("item=%+v err=%v", item, err)
	}

	ev.Title = "Inception (2010)"
	ev.EventType = "playback.progress"
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	item, err = m.getLibraryItem(ctx, "jf1", "item-99")
	if err != nil || item == nil || item.Title != "Inception (2010)" {
		t.Fatalf("updated item=%+v err=%v", item, err)
	}
}

func TestPublicUsersAPI(t *testing.T) {
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
		ExternalSessionID: "u1-1",
		UserID:            "uid-alice",
		UserName:          "Alice",
		ItemID:            "m1",
		Title:             "Movie A",
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	ev.EventType = "playback.stopped"
	ev.PositionSeconds = 600
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}

	listRec := httptest.NewRecorder()
	listReq := httptest.NewRequest(http.MethodGet, "/api/v2/public/users", nil)
	listReq.Header.Set("Authorization", "Bearer test-public-key")
	listReq.SetPathValue("id", "")
	m.handlePublicUsers(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("users list: %d %s", listRec.Code, listRec.Body.String())
	}
	var listBody map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listBody); err != nil {
		t.Fatal(err)
	}
	data, ok := listBody["data"].([]any)
	if !ok || len(data) != 1 {
		t.Fatalf("expected 1 user, got %#v", listBody["data"])
	}

	statsRec := httptest.NewRecorder()
	statsReq := httptest.NewRequest(http.MethodGet, "/api/v2/public/users/uid-alice/stats", nil)
	statsReq.Header.Set("Authorization", "Bearer test-public-key")
	statsReq.SetPathValue("id", "uid-alice")
	m.handlePublicUserStats(statsRec, statsReq)
	if statsRec.Code != http.StatusOK {
		t.Fatalf("user stats: %d %s", statsRec.Code, statsRec.Body.String())
	}
	var statsBody map[string]any
	if err := json.Unmarshal(statsRec.Body.Bytes(), &statsBody); err != nil {
		t.Fatal(err)
	}
	windows, ok := statsBody["windows"].(map[string]any)
	if !ok {
		t.Fatalf("windows: %#v", statsBody["windows"])
	}
	allTime, ok := windows["all_time"].(map[string]any)
	if !ok {
		t.Fatalf("all_time: %#v", windows["all_time"])
	}
	if plays, ok := allTime["plays"].(float64); !ok || int(plays) != 1 {
		t.Fatalf("plays: %#v", allTime["plays"])
	}

	histRec := httptest.NewRecorder()
	histReq := httptest.NewRequest(http.MethodGet, "/api/v2/public/users/uid-alice/history?pageSize=10", nil)
	histReq.Header.Set("Authorization", "Bearer test-public-key")
	histReq.SetPathValue("id", "uid-alice")
	m.handlePublicUserHistory(histRec, histReq)
	if histRec.Code != http.StatusOK {
		t.Fatalf("user history: %d %s", histRec.Code, histRec.Body.String())
	}
}
