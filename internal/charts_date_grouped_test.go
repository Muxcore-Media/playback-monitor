package internal

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPlaysByDateChartGroupedByMediaType(t *testing.T) {
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

	identityUserID := "user-chart-filter"
	now := time.Now().UTC()
	day := now.Format("2006-01-02")
	insertStoppedSession(t, ctx, m, sessionFixture{
		userID:    identityUserID,
		mediaType: "movie",
		title:     "Movie A",
		startedAt: day + "T12:00:00Z",
	})
	insertStoppedSession(t, ctx, m, sessionFixture{
		userID:    identityUserID,
		mediaType: "episode",
		title:     "Show S1E1",
		startedAt: day + "T13:00:00Z",
	})
	var identityID string
	if err := m.queryRow(ctx, `SELECT identity_id FROM sessions WHERE user_id = ? LIMIT 1`, identityUserID).Scan(&identityID); err != nil || identityID == "" {
		t.Fatalf("identity_id lookup: err=%v id=%q", err, identityID)
	}

	chart, err := m.playsByDateChart(ctx, playsByDateChartOpts{
		Days:     7,
		UserIDs:  []string{identityID},
		Grouping: true,
		YAxis:    "plays",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(chart.Categories) != 7 {
		t.Fatalf("categories: %d", len(chart.Categories))
	}
	foundTV, foundMovies := false, false
	for _, s := range chart.Series {
		switch s.Name {
		case "TV":
			foundTV = true
			if sumInts(s.Data) != 1 {
				t.Fatalf("TV data sum: %d", sumInts(s.Data))
			}
		case "Movies":
			foundMovies = true
			if sumInts(s.Data) != 1 {
				t.Fatalf("Movies data sum: %d", sumInts(s.Data))
			}
		case "Total":
			if sumInts(s.Data) != 2 {
				t.Fatalf("Total data sum: %d", sumInts(s.Data))
			}
		}
	}
	if !foundTV || !foundMovies {
		t.Fatalf("series: %#v", chart.Series)
	}
}

func TestPlaysByDateChartDurationAxis(t *testing.T) {
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

	day := time.Now().UTC().Format("2006-01-02")
	insertStoppedSession(t, ctx, m, sessionFixture{
		mediaType:       "movie",
		title:           "Long Movie",
		startedAt:       day + "T10:00:00Z",
		positionSeconds: 3600,
	})

	chart, err := m.playsByDateChart(ctx, playsByDateChartOpts{Days: 7, YAxis: "duration"})
	if err != nil {
		t.Fatal(err)
	}
	var total int
	for _, s := range chart.Series {
		if s.Name == "Total" {
			total = sumInts(s.Data)
		}
	}
	if total != 3600 {
		t.Fatalf("expected 3600 duration seconds, got %d", total)
	}
}

type sessionFixture struct {
	userID          string
	mediaType       string
	title           string
	startedAt       string
	positionSeconds float64
}

func insertStoppedSession(t *testing.T, ctx context.Context, m *Module, fix sessionFixture) {
	t.Helper()
	ev := SessionEvent{
		EventType:         "playback.started",
		SourceModule:      "jellyfin",
		ExternalSessionID: uuid.NewString(),
		UserID:            fix.userID,
		UserName:          "tester",
		ItemID:            uuid.NewString(),
		Title:             fix.title,
		MediaType:         fix.mediaType,
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	ev.EventType = "playback.stopped"
	ev.PositionSeconds = int64(fix.positionSeconds)
	if fix.positionSeconds == 0 {
		ev.PositionSeconds = 120
	}
	if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
}

func sumInts(vals []int) int {
	n := 0
	for _, v := range vals {
		n += v
	}
	return n
}
