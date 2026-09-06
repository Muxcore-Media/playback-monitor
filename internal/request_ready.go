package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
)

// EventRequestReady is published when a user-requested title becomes playable
// (library has_file). Same semantics as media-ui ready toasts — not status=available.
// Canonical constant lives in contracts-media/events (EventRequestReady).
const EventRequestReady = "media.request.ready"

type requestReadyPayload struct {
	RequestID   string `json:"request_id"`
	RequestedBy string `json:"requested_by"`
	Title       string `json:"title"`
	Year        string `json:"year"`
	ItemType    string `json:"item_type"`
	TMDBID      string `json:"tmdb_id"`
	ItemID      string `json:"item_id"`
}

func parseRequestReadyPayload(raw []byte) (requestReadyPayload, error) {
	var loose struct {
		RequestID   string          `json:"request_id"`
		RequestedBy string          `json:"requested_by"`
		Title       string          `json:"title"`
		ItemType    string          `json:"item_type"`
		ItemID      string          `json:"item_id"`
		Year        json.RawMessage `json:"year"`
		TMDBID      json.RawMessage `json:"tmdb_id"`
	}
	if err := json.Unmarshal(raw, &loose); err != nil {
		return requestReadyPayload{}, err
	}
	return requestReadyPayload{
		RequestID:   strings.TrimSpace(loose.RequestID),
		RequestedBy: strings.TrimSpace(loose.RequestedBy),
		Title:       strings.TrimSpace(loose.Title),
		Year:        jsonScalarString(loose.Year),
		ItemType:    strings.TrimSpace(loose.ItemType),
		TMDBID:      jsonScalarString(loose.TMDBID),
		ItemID:      strings.TrimSpace(loose.ItemID),
	}, nil
}

func jsonScalarString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s)
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String()
	}
	return strings.TrimSpace(string(raw))
}

func (m *Module) handleRequestReadyEvent(ctx context.Context, evt *eventsv1.Event) {
	if evt == nil || len(evt.Payload) == 0 {
		return
	}
	payload, err := parseRequestReadyPayload(evt.Payload)
	if err != nil {
		slog.Debug("playback-monitor: bad request-ready payload", "error", err)
		return
	}
	if payload.RequestID == "" {
		return
	}
	claimed, err := m.claimRequestReadyDispatch(ctx, payload.RequestID)
	if err != nil {
		slog.Debug("playback-monitor: request-ready dedupe failed", "request_id", payload.RequestID, "error", err)
		return
	}
	if !claimed {
		return
	}
	m.fireRequestReadyNotificationRules(ctx, payload)
}

func (m *Module) fireRequestReadyNotificationRules(ctx context.Context, payload requestReadyPayload) {
	rules, err := m.listNotificationRules(ctx, EventRequestReady)
	if err != nil {
		return
	}
	vars := requestReadyNotificationVars(payload)
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		if !notificationRuleMatchesRequestReady(rule, payload) {
			continue
		}
		m.dispatchNotificationRule(ctx, rule, vars)
	}
}

func (m *Module) claimRequestReadyDispatch(ctx context.Context, requestID string) (bool, error) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return false, nil
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return false, fmt.Errorf("db not initialized")
	}
	res, err := m.exec(ctx, `
		INSERT INTO request_ready_dispatched(request_id, dispatched_at)
		VALUES (?, ?)
		ON CONFLICT(request_id) DO NOTHING`,
		requestID, time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (m *Module) countRequestReadyDispatched(ctx context.Context) (int, error) {
	var n int
	err := m.queryRow(ctx, `SELECT COUNT(1) FROM request_ready_dispatched`).Scan(&n)
	return n, err
}
