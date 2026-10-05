package integsupport_test

import (
	"context"
	"testing"

	"github.com/Muxcore-Media/playback-monitor/integsupport"
	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

func TestNewTestModuleHealthAndStart(t *testing.T) {
	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := integsupport.NewTestModule(t, integsupport.Config{})
	if err := m.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}
	if err := integsupport.Start(ctx, m); err != nil {
		t.Fatalf("start: %v", err)
	}
}

func TestIngestAndQuery(t *testing.T) {
	ctx := context.Background()
	m := integsupport.NewTestModule(t, integsupport.Config{})
	ev := integsupport.SessionEvent{
		ServerID: "srv", ExternalSessionID: "a", UserID: "alice", UserName: "Alice",
		ItemID: "i1", Title: "A", Platform: "Android", IsTranscode: true, PositionSeconds: 3600,
		EventType: "playback.started",
	}
	if _, created, err := integsupport.IngestSession(ctx, m, ev); err != nil || !created {
		t.Fatalf("start: created=%v err=%v", created, err)
	}
	ev.EventType = "playback.stopped"
	if _, _, err := integsupport.IngestSession(ctx, m, ev); err != nil {
		t.Fatalf("stop: %v", err)
	}
	h, err := m.ListHistory(ctx, &monitorv1.ListHistoryRequest{Limit: 5})
	if err != nil || len(h.GetSessions()) != 1 {
		t.Fatalf("history: %v err=%v", h.GetSessions(), err)
	}
	if _, err := m.GetHomeStats(ctx, &monitorv1.GetHomeStatsRequest{Days: 7}); err != nil {
		t.Fatalf("home stats: %v", err)
	}
	if _, err := m.GetStreamAnalytics(ctx, &monitorv1.GetStreamAnalyticsRequest{Days: 7}); err != nil {
		t.Fatalf("analytics: %v", err)
	}
}
