package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestPublicMediaChildren(t *testing.T) {
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

	for _, ev := range []libraryCatalogEvent{
		{ServerID: "s1", ServerType: "jellyfin", ItemID: "show1", Title: "Show", MediaType: "Series", LibraryName: "TV"},
		{ServerID: "s1", ServerType: "jellyfin", ItemID: "s1", Title: "Season 1", MediaType: "Season", ParentID: "show1", LibraryName: "TV"},
		{ServerID: "s1", ServerType: "jellyfin", ItemID: "e1", Title: "Ep 1", MediaType: "Episode", ParentID: "s1", LibraryName: "TV", VideoResolution: "1080"},
	} {
		if err := m.upsertLibraryCatalogItem(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v2/public/media/s1:show1/children", nil)
	req.SetPathValue("ref", "s1:show1")
	req.Header.Set("Authorization", "Bearer test-public-key")
	m.handlePublicMediaChildren(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("children: %d %s", rec.Code, rec.Body.String())
	}

	showRec := httptest.NewRecorder()
	showReq := httptest.NewRequest(http.MethodGet, "/api/v2/public/media/s1:show1", nil)
	showReq.SetPathValue("ref", "s1:show1")
	showReq.Header.Set("Authorization", "Bearer test-public-key")
	m.handlePublicMedia(showRec, showReq)
	if showRec.Code != http.StatusOK {
		t.Fatalf("media: %d %s", showRec.Code, showRec.Body.String())
	}

	if err := m.upsertLibraryCatalogItem(ctx, libraryCatalogEvent{
		ServerID: "s1", ItemID: "movie1", Title: "Film", MediaType: "Movie",
	}); err != nil {
		t.Fatal(err)
	}
	movieRec := httptest.NewRecorder()
	movieReq := httptest.NewRequest(http.MethodGet, "/api/v2/public/media/s1:movie1/children", nil)
	movieReq.SetPathValue("ref", "s1:movie1")
	movieReq.Header.Set("Authorization", "Bearer test-public-key")
	m.handlePublicMediaChildren(movieRec, movieReq)
	if movieRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for movie children, got %d", movieRec.Code)
	}
}
