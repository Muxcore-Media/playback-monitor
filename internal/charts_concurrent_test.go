package internal

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestPeakConcurrent(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	intervals := []concurrentInterval{
		{start: base, stop: base.Add(2 * time.Hour)},
		{start: base.Add(30 * time.Minute), stop: base.Add(90 * time.Minute)},
		{start: base.Add(3 * time.Hour), stop: base.Add(4 * time.Hour)},
	}
	if got := peakConcurrent(intervals); got != 2 {
		t.Fatalf("peak=%d want 2", got)
	}
}

func TestConcurrentStreamsByStreamType(t *testing.T) {
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

	base := time.Now().UTC().Add(-2 * time.Hour)
	day := base.Format("2006-01-02")
	start1 := time.Date(base.Year(), base.Month(), base.Day(), 10, 0, 0, 0, time.UTC).Format(time.RFC3339)
	stop1 := time.Date(base.Year(), base.Month(), base.Day(), 11, 0, 0, 0, time.UTC).Format(time.RFC3339)
	start2 := time.Date(base.Year(), base.Month(), base.Day(), 10, 30, 0, 0, time.UTC).Format(time.RFC3339)
	stop2 := time.Date(base.Year(), base.Month(), base.Day(), 11, 30, 0, 0, time.UTC).Format(time.RFC3339)

	for _, ev := range []SessionEvent{
		{EventType: "playback.started", ServerID: "s1", ExternalSessionID: "c1", ItemID: "i1", IsTranscode: false, OccurredAt: mustParseTime(start1)},
		{EventType: "playback.started", ServerID: "s1", ExternalSessionID: "c2", ItemID: "i2", IsTranscode: true, OccurredAt: mustParseTime(start2)},
	} {
		if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	stopEv1 := SessionEvent{EventType: "playback.stopped", ServerID: "s1", ExternalSessionID: "c1", ItemID: "i1", OccurredAt: mustParseTime(stop1)}
	stopEv2 := SessionEvent{EventType: "playback.stopped", ServerID: "s1", ExternalSessionID: "c2", ItemID: "i2", OccurredAt: mustParseTime(stop2)}
	if _, _, err := m.ingestSessionEvent(ctx, stopEv1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ingestSessionEvent(ctx, stopEv2); err != nil {
		t.Fatal(err)
	}

	chart, err := m.concurrentStreamsByStreamType(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(chart.Categories) == 0 || len(chart.Series) != 3 {
		t.Fatalf("chart=%+v", chart)
	}
	idx := -1
	for i, d := range chart.Categories {
		if d == day {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatalf("day %s not in categories %v", day, chart.Categories)
	}
	if chart.Series[0].Data[idx] < 2 {
		t.Fatalf("total peak: %d", chart.Series[0].Data[idx])
	}
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
