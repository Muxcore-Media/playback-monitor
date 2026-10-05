package internal

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNotificationDestinationsCRUDAndDelivery(t *testing.T) {
	allowLocalWebhooks(t)
	ctx := context.Background()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "hello") {
			t.Errorf("unexpected payload: %s", body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

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

	dest, err := m.upsertNotificationDestination(ctx, NotificationDestination{
		Name:    "Discord ops",
		Type:    "discord",
		Enabled: true,
		Config:  map[string]string{"webhook_url": srv.URL},
		Events:  []string{"playback.started"},
	})
	if err != nil {
		t.Fatal(err)
	}

	rule, err := m.upsertNotificationRule(ctx, NotificationRule{
		Name:            "Start ping",
		Enabled:         true,
		EventType:       "playback.started",
		TitleTemplate:   "Start",
		MessageTemplate: "hello",
		Severity:        "info",
		DestinationIDs:  []string{dest.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	m.dispatchNotificationRule(ctx, rule, map[string]string{"user": "alice", "title": "Movie"})
	if hits.Load() != 1 {
		t.Fatalf("expected webhook delivery, hits=%d", hits.Load())
	}

	req := httptest.NewRequest(http.MethodGet, "/notification/destinations", nil)
	rec := httptest.NewRecorder()
	m.handleListNotificationDestinations(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list destinations: %d %s", rec.Code, rec.Body.String())
	}
	var listBody map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &listBody); err != nil {
		t.Fatal(err)
	}
	dests, ok := listBody["destinations"].([]any)
	if !ok || len(dests) != 1 {
		t.Fatalf("destinations: %#v", listBody["destinations"])
	}

	if err := m.deleteNotificationDestination(ctx, dest.ID); err != nil {
		t.Fatal(err)
	}
	rules, err := m.listNotificationRules(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rules {
		if len(r.DestinationIDs) != 0 {
			t.Fatalf("expected destination removed from rule, got %#v", r.DestinationIDs)
		}
	}
}

func TestAssertSafeWebhookURLBlocksLoopback(t *testing.T) {
	if err := assertSafeWebhookURL("http://127.0.0.1/hook"); err == nil {
		t.Fatal("expected loopback block")
	}
}
