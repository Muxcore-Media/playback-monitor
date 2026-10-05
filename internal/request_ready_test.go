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

	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
)

func TestIsAllowedNotificationEventTypeRequestReady(t *testing.T) {
	if !isAllowedNotificationEventType(EventRequestReady) {
		t.Fatal("expected media.request.ready to be allowed")
	}
	if !isAllowedNotificationEventType("media.request.ready") {
		t.Fatal("expected literal media.request.ready to be allowed")
	}
	if isAllowedNotificationEventType("media.request.available") {
		t.Fatal("status=available alias must not be allowed")
	}
}

func TestRequestReadyDestinationAndRuleAccepted(t *testing.T) {
	allowLocalWebhooks(t)
	ctx := context.Background()
	m := newTestMonitor(t)

	dest, err := m.upsertNotificationDestination(ctx, NotificationDestination{
		Name:    "Household Discord",
		Type:    "discord",
		Enabled: true,
		Config:  map[string]string{"webhook_url": "http://127.0.0.1/hook"},
		Events:  []string{EventRequestReady},
	})
	if err != nil {
		t.Fatalf("destination: %v", err)
	}

	if _, err := m.upsertNotificationRule(ctx, NotificationRule{
		Name:            "Library ready",
		Enabled:         true,
		EventType:       EventRequestReady,
		TitleTemplate:   "{title} is ready to watch",
		MessageTemplate: "{requested_by} requested \"{title}\"",
		Severity:        "success",
		DestinationIDs:  []string{dest.ID},
	}); err != nil {
		t.Fatalf("rule: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/notification/destinations", strings.NewReader(`{
		"name":"Webhook ready",
		"type":"webhook",
		"enabled":true,
		"config":{"webhook_url":"http://127.0.0.1/hook"},
		"events":["media.request.ready"]
	}`))
	rec := httptest.NewRecorder()
	m.handleUpsertNotificationDestination(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("destination HTTP: %d %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/notification/rules", strings.NewReader(`{
		"name":"Ready ping",
		"enabled":true,
		"event_type":"media.request.ready",
		"title_template":"{title} is ready",
		"message_template":"{request_id}",
		"severity":"success"
	}`))
	rec = httptest.NewRecorder()
	m.handleUpsertNotificationRule(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rule HTTP: %d %s", rec.Code, rec.Body.String())
	}
}

func TestRequestReadyDispatchAndDedupe(t *testing.T) {
	allowLocalWebhooks(t)
	ctx := context.Background()
	var hits atomic.Int32
	var lastBody atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		lastBody.Store(string(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	m := newTestMonitor(t)
	dest, err := m.upsertNotificationDestination(ctx, NotificationDestination{
		Name:    "Discord ready",
		Type:    "discord",
		Enabled: true,
		Config:  map[string]string{"webhook_url": srv.URL},
		Events:  []string{EventRequestReady},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.upsertNotificationRule(ctx, NotificationRule{
		Name:            "Ready",
		Enabled:         true,
		EventType:       EventRequestReady,
		TitleTemplate:   "{title} is ready to watch",
		MessageTemplate: "{requested_by} requested \"{title}\" ({year})",
		Severity:        "success",
		DestinationIDs:  []string{dest.ID},
	}); err != nil {
		t.Fatal(err)
	}

	payload, _ := json.Marshal(map[string]any{
		"request_id":   "req_mv_1",
		"requested_by": "alice",
		"title":        "Inception",
		"year":         2010,
		"item_type":    "movie",
		"tmdb_id":      27205,
		"item_id":      "movie-1",
	})
	evt := &eventsv1.Event{Type: EventRequestReady, Source: "request-media", Payload: payload}
	m.handleRequestReadyEvent(ctx, evt)
	m.handleRequestReadyEvent(ctx, evt) // reload / re-emit

	if hits.Load() != 1 {
		t.Fatalf("expected one webhook delivery, hits=%d", hits.Load())
	}
	body, _ := lastBody.Load().(string)
	if !strings.Contains(body, "Inception is ready to watch") {
		t.Fatalf("payload missing title: %s", body)
	}
	if !strings.Contains(body, "alice") || !strings.Contains(body, "req_mv_1") {
		t.Fatalf("payload missing request fields: %s", body)
	}
}

func TestRequestReadyUnconfiguredDestinationIsQuiet(t *testing.T) {
	ctx := context.Background()
	m := newTestMonitor(t)
	if _, err := m.upsertNotificationRule(ctx, NotificationRule{
		Name:            "Ready no dest",
		Enabled:         true,
		EventType:       EventRequestReady,
		TitleTemplate:   "{title} is ready",
		MessageTemplate: "{request_id}",
		Severity:        "success",
	}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"request_id": "req_quiet", "title": "Quiet"})
	m.handleRequestReadyEvent(ctx, &eventsv1.Event{Payload: payload})
	claimed, err := m.claimRequestReadyDispatch(ctx, "req_quiet")
	if err != nil {
		t.Fatal(err)
	}
	if claimed {
		t.Fatal("expected request_id to already be claimed after quiet dispatch")
	}
}

func TestRequestReadySkipsEmptyRequestID(t *testing.T) {
	ctx := context.Background()
	m := newTestMonitor(t)
	m.handleRequestReadyEvent(ctx, &eventsv1.Event{Payload: []byte(`{"title":"No ID"}`)})
	if n, err := m.countRequestReadyDispatched(ctx); err != nil || n != 0 {
		t.Fatalf("expected no dedupe rows, n=%d err=%v", n, err)
	}
}

func TestNotificationRuleMatchesRequestReady(t *testing.T) {
	rule := NotificationRule{Filters: NotificationRuleFilters{UserIDs: []string{"alice"}, MediaTypes: []string{"movie"}}}
	payload := requestReadyPayload{RequestedBy: "alice", ItemType: "movie"}
	if !notificationRuleMatchesRequestReady(rule, payload) {
		t.Fatal("expected match")
	}
	payload.RequestedBy = "bob"
	if notificationRuleMatchesRequestReady(rule, payload) {
		t.Fatal("expected user filter miss")
	}
	payload.RequestedBy = "alice"
	payload.ItemType = "tv"
	if notificationRuleMatchesRequestReady(rule, payload) {
		t.Fatal("expected media type filter miss")
	}
}

func newTestMonitor(t *testing.T) *Module {
	t.Helper()
	ctx := context.Background()
	m := NewModule(Config{
		DBPath:   filepath.Join(t.TempDir(), "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })
	return m
}
