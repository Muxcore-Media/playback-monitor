package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestPublicHistoryFilters(t *testing.T) {
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

	start := SessionEvent{
		EventType: "playback.started", ServerID: "s1", ExternalSessionID: "a1",
		UserID: "u1", UserName: "alice", ItemID: "movie-1", MuxcoreID: "mc-1", Title: "Movie A", MediaType: "Movie",
	}
	if _, _, err := m.ingestSessionEvent(ctx, start); err != nil {
		t.Fatal(err)
	}
	stop := start
	stop.EventType = "playback.stopped"
	stop.PositionSeconds = 120
	if _, _, err := m.ingestSessionEvent(ctx, stop); err != nil {
		t.Fatal(err)
	}

	start2 := start
	start2.ExternalSessionID = "a2"
	start2.ItemID = "movie-2"
	start2.MuxcoreID = "mc-2"
	start2.Title = "Movie B"
	if _, _, err := m.ingestSessionEvent(ctx, start2); err != nil {
		t.Fatal(err)
	}
	stop2 := start2
	stop2.EventType = "playback.stopped"
	stop2.PositionSeconds = 5
	if _, _, err := m.ingestSessionEvent(ctx, stop2); err != nil {
		t.Fatal(err)
	}

	auth := func(url string) []SessionRecord {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("Authorization", "Bearer test-public-key")
		m.handlePublicHistory(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: %d %s", url, rec.Code, rec.Body.String())
		}
		filter, err := parseHistoryPageFilter(req)
		if err != nil {
			t.Fatal(err)
		}
		rows, _, err := m.listHistoryPageFiltered(ctx, filter, 25, "")
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}

	if rows := auth("/api/v2/public/history?rating_key=movie-1"); len(rows) != 1 || rows[0].ItemID != "movie-1" {
		t.Fatalf("rating_key filter: %+v", rows)
	}
	if rows := auth("/api/v2/public/history?media_id=mc-2"); len(rows) != 1 || rows[0].MuxcoreID != "mc-2" {
		t.Fatalf("media_id filter: %+v", rows)
	}

	watchedTrue := true
	rows, _, err := m.listHistoryPageFiltered(ctx, historyPageFilter{Watched: &watchedTrue}, 25, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ItemID != "movie-1" {
		t.Fatalf("watched filter: %+v", rows)
	}

	idents, _, err := m.listPublicUserIdentities(ctx, 10, "")
	if err != nil || len(idents) != 1 {
		t.Fatalf("idents: %+v err=%v", idents, err)
	}
	if _, err := uuid.Parse(idents[0].ID); err != nil {
		t.Fatalf("identity uuid: %q", idents[0].ID)
	}
	rows, _, err = m.listHistoryPageFiltered(ctx, historyPageFilter{IdentityID: idents[0].ID}, 25, "")
	if err != nil || len(rows) != 2 {
		t.Fatalf("identity filter: %+v err=%v", rows, err)
	}
}
