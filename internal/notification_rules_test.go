package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestNotificationRulesCRUDAndMatch(t *testing.T) {
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

	rule, err := m.upsertNotificationRule(ctx, NotificationRule{
		Name:            "Transcode starts",
		Enabled:         true,
		EventType:       "playback.started",
		TitleTemplate:   "Transcode",
		MessageTemplate: "{user} -> {title}",
		Severity:        "warning",
		Filters:         NotificationRuleFilters{TranscodeOnly: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	ev := SessionEvent{
		UserName:    "alice",
		Title:       "Big Movie",
		IsTranscode: true,
		Platform:    "web",
	}
	if !notificationRuleMatchesSession(rule, ev) {
		t.Fatal("expected transcode rule to match")
	}
	ev.IsTranscode = false
	if notificationRuleMatchesSession(rule, ev) {
		t.Fatal("expected non-transcode to skip rule")
	}

	req := httptest.NewRequest(http.MethodGet, "/notification/rules", nil)
	rec := httptest.NewRecorder()
	m.handleListNotificationRules(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
	var listBody map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &listBody); err != nil {
		t.Fatal(err)
	}
	rules, ok := listBody["rules"].([]any)
	if !ok || len(rules) != 1 {
		t.Fatalf("rules: %#v", listBody["rules"])
	}
}

func TestGuardViolationNotificationRules(t *testing.T) {
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

	if _, err := m.upsertNotificationRule(ctx, NotificationRule{
		Name:            "Guard alerts",
		Enabled:         true,
		EventType:       "guard.violation",
		TitleTemplate:   "Sharing alert",
		MessageTemplate: "{user}: {summary}",
		Severity:        "warning",
	}); err != nil {
		t.Fatal(err)
	}

	rules, err := m.listNotificationRules(ctx, "guard.violation")
	if err != nil || len(rules) != 1 {
		t.Fatalf("rules: err=%v len=%d", err, len(rules))
	}
	payload := guardViolationPayload{
		User:     "bob",
		Summary:  "too many streams",
		RuleType: "concurrent_streams",
	}
	if !notificationRuleMatchesGuard(rules[0], payload) {
		t.Fatal("expected guard rule to match")
	}
}

func TestRenderNotificationTemplate(t *testing.T) {
	out := renderNotificationTemplate("{user} watched {title} on {platform}", map[string]string{
		"user":     "alice",
		"title":    "Inception",
		"platform": "iOS",
	})
	if out != "alice watched Inception on iOS" {
		t.Fatalf("got %q", out)
	}
}
