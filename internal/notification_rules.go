package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
	"github.com/google/uuid"
)

type NotificationRuleFilters struct {
	UserIDs        []string `json:"user_ids,omitempty"`
	Platforms      []string `json:"platforms,omitempty"`
	MediaTypes     []string `json:"media_types,omitempty"`
	TranscodeOnly  bool     `json:"transcode_only,omitempty"`
	MinDurationSec int64    `json:"min_duration_sec,omitempty"`
}

type NotificationRule struct {
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ID              string
	Name            string
	EventType       string
	TitleTemplate   string
	MessageTemplate string
	Severity        string
	DestinationIDs  []string
	Filters         NotificationRuleFilters
	Enabled         bool
}

func (m *Module) seedDefaultNotificationRules(ctx context.Context) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil
	}
	var n int
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM notification_rules`).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if m.getNotifyOnSessionStart() {
		if err := m.insertNotificationRuleRow(ctx, NotificationRule{
			ID:              uuid.NewString(),
			Name:            "Session start (default)",
			Enabled:         true,
			EventType:       playbackevents.EventPlaybackStarted,
			TitleTemplate:   "Playback started",
			MessageTemplate: "{user} started watching \"{title}\"",
			Severity:        "info",
		}, now); err != nil {
			return err
		}
	}
	if m.getNotifyOnSessionStop() {
		if err := m.insertNotificationRuleRow(ctx, NotificationRule{
			ID:              uuid.NewString(),
			Name:            "Session stop (default)",
			Enabled:         true,
			EventType:       playbackevents.EventPlaybackStopped,
			TitleTemplate:   "Playback stopped",
			MessageTemplate: "{user} finished watching \"{title}\" ({position} watched)",
			Severity:        "info",
		}, now); err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) insertNotificationRuleRow(ctx context.Context, rule NotificationRule, now string) error {
	filtersJSON, err := json.Marshal(rule.Filters)
	if err != nil {
		return err
	}
	destJSON, err := json.Marshal(rule.DestinationIDs)
	if err != nil {
		return err
	}
	_, err = m.exec(ctx, `
		INSERT INTO notification_rules(
			id, name, enabled, event_type, title_template, message_template, severity, filters_json, destination_ids_json, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rule.ID, rule.Name, boolToInt(rule.Enabled), rule.EventType,
		rule.TitleTemplate, rule.MessageTemplate, rule.Severity, string(filtersJSON), string(destJSON), now, now,
	)
	return err
}

func (m *Module) listNotificationRules(ctx context.Context, eventType string) ([]NotificationRule, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	query := `SELECT id, name, enabled, event_type, title_template, message_template, severity, filters_json, destination_ids_json, created_at, updated_at
		FROM notification_rules`
	args := []any{}
	if strings.TrimSpace(eventType) != "" {
		query += ` WHERE event_type = ?`
		args = append(args, eventType)
	}
	query += ` ORDER BY event_type ASC, name ASC`
	rows, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanNotificationRules(rows)
}

func scanNotificationRules(rows *sql.Rows) ([]NotificationRule, error) {
	out := make([]NotificationRule, 0)
	for rows.Next() {
		var rule NotificationRule
		var enabled int
		var filtersJSON, destJSON, createdAt, updatedAt string
		if err := rows.Scan(
			&rule.ID, &rule.Name, &enabled, &rule.EventType,
			&rule.TitleTemplate, &rule.MessageTemplate, &rule.Severity,
			&filtersJSON, &destJSON, &createdAt, &updatedAt,
		); err != nil {
			return nil, err
		}
		rule.Enabled = enabled == 1
		rule.CreatedAt = parseTime(createdAt)
		rule.UpdatedAt = parseTime(updatedAt)
		if filtersJSON != "" {
			_ = json.Unmarshal([]byte(filtersJSON), &rule.Filters)
		}
		if destJSON != "" {
			_ = json.Unmarshal([]byte(destJSON), &rule.DestinationIDs)
		}
		out = append(out, rule)
	}
	return out, rows.Err()
}

func (m *Module) upsertNotificationRule(ctx context.Context, rule NotificationRule) (NotificationRule, error) {
	if strings.TrimSpace(rule.Name) == "" {
		return NotificationRule{}, fmt.Errorf("name required")
	}
	if strings.TrimSpace(rule.EventType) == "" {
		return NotificationRule{}, fmt.Errorf("event_type required")
	}
	if !isAllowedNotificationEventType(rule.EventType) {
		return NotificationRule{}, fmt.Errorf("unsupported event_type")
	}
	if rule.Severity == "" {
		rule.Severity = "info"
	}
	if rule.ID == "" {
		rule.ID = uuid.NewString()
	}
	now := time.Now().UTC().Format(time.RFC3339)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return NotificationRule{}, fmt.Errorf("db not initialized")
	}
	filtersJSON, err := json.Marshal(rule.Filters)
	if err != nil {
		return NotificationRule{}, err
	}
	destJSON, err := json.Marshal(rule.DestinationIDs)
	if err != nil {
		return NotificationRule{}, err
	}
	var existingCreated string
	_ = m.queryRow(ctx, `SELECT created_at FROM notification_rules WHERE id = ?`, rule.ID).Scan(&existingCreated)
	createdAt := now
	if existingCreated != "" {
		createdAt = existingCreated
	}
	_, err = m.exec(ctx, `
		INSERT INTO notification_rules(
			id, name, enabled, event_type, title_template, message_template, severity, filters_json, destination_ids_json, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			enabled = excluded.enabled,
			event_type = excluded.event_type,
			title_template = excluded.title_template,
			message_template = excluded.message_template,
			severity = excluded.severity,
			filters_json = excluded.filters_json,
			destination_ids_json = excluded.destination_ids_json,
			updated_at = excluded.updated_at`,
		rule.ID, rule.Name, boolToInt(rule.Enabled), rule.EventType,
		rule.TitleTemplate, rule.MessageTemplate, rule.Severity, string(filtersJSON), string(destJSON), createdAt, now,
	)
	if err != nil {
		return NotificationRule{}, err
	}
	rules, listErr := m.listNotificationRules(ctx, "")
	if listErr != nil {
		return NotificationRule{}, listErr
	}
	for _, r := range rules {
		if r.ID == rule.ID {
			return r, nil
		}
	}
	return rule, nil
}

func (m *Module) deleteNotificationRule(ctx context.Context, id string) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	res, err := m.exec(ctx, `DELETE FROM notification_rules WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func isAllowedNotificationEventType(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case playbackevents.EventPlaybackStarted, playbackevents.EventPlaybackStopped, "guard.violation", EventRequestReady:
		return true
	default:
		return false
	}
}

func (m *Module) firePlaybackNotificationRules(ctx context.Context, eventType string, ev SessionEvent) {
	rules, err := m.listNotificationRules(ctx, eventType)
	if err != nil {
		return
	}
	matched := false
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		if !notificationRuleMatchesSession(rule, ev) {
			continue
		}
		matched = true
		m.dispatchNotificationRule(ctx, rule, sessionNotificationVars(ev))
	}
	if !matched {
		m.fireLegacyPlaybackNotification(ctx, eventType, ev)
	}
}

func (m *Module) fireGuardViolationNotificationRules(ctx context.Context, payload guardViolationPayload) {
	rules, err := m.listNotificationRules(ctx, "guard.violation")
	if err != nil {
		return
	}
	matched := false
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		if !notificationRuleMatchesGuard(rule, payload) {
			continue
		}
		matched = true
		m.dispatchNotificationRule(ctx, rule, guardNotificationVars(payload))
	}
	if !matched && payload.LegacyNotify {
		m.postNotification(ctx, "Playback guard violation", payload.Summary, "warning", map[string]string{
			"event":     "guard.violation",
			"user":      payload.User,
			"rule_type": payload.RuleType,
		})
	}
}

func notificationRuleMatchesSession(rule NotificationRule, ev SessionEvent) bool {
	f := rule.Filters
	if f.TranscodeOnly && !ev.IsTranscode {
		return false
	}
	if f.MinDurationSec > 0 && ev.PositionSeconds < f.MinDurationSec {
		return false
	}
	if len(f.UserIDs) > 0 && !containsFold(f.UserIDs, ev.IdentityID) {
		return false
	}
	if len(f.Platforms) > 0 && !containsFold(f.Platforms, ev.Platform) {
		return false
	}
	if len(f.MediaTypes) > 0 && !containsFold(f.MediaTypes, ev.MediaType) {
		return false
	}
	return true
}

type guardViolationPayload struct {
	User         string `json:"user"`
	Summary      string `json:"summary"`
	RuleType     string `json:"rule_type"`
	LegacyNotify bool   `json:"legacy_notify,omitempty"`
}

func notificationRuleMatchesGuard(rule NotificationRule, payload guardViolationPayload) bool {
	f := rule.Filters
	if len(f.Platforms) > 0 || len(f.MediaTypes) > 0 || f.TranscodeOnly || f.MinDurationSec > 0 {
		return false
	}
	if len(f.UserIDs) > 0 && !containsFold(f.UserIDs, payload.User) {
		return false
	}
	return true
}

func notificationRuleMatchesRequestReady(rule NotificationRule, payload requestReadyPayload) bool {
	f := rule.Filters
	if f.TranscodeOnly || f.MinDurationSec > 0 || len(f.Platforms) > 0 {
		return false
	}
	if len(f.UserIDs) > 0 && !containsFold(f.UserIDs, payload.RequestedBy) {
		return false
	}
	if len(f.MediaTypes) > 0 && !containsFold(f.MediaTypes, payload.ItemType) {
		return false
	}
	return true
}

func containsFold(list []string, value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, item := range list {
		if strings.EqualFold(strings.TrimSpace(item), value) {
			return true
		}
	}
	return false
}

func sessionNotificationVars(ev SessionEvent) map[string]string {
	user := firstNonEmptyStr(ev.UserName, ev.UserID, "unknown")
	title := ev.Title
	if title == "" {
		title = ev.ItemID
	}
	return map[string]string{
		"user":        user,
		"title":       title,
		"platform":    ev.Platform,
		"device":      ev.Device,
		"media_type":  ev.MediaType,
		"position":    formatMinutes(ev.PositionSeconds),
		"session_id":  ev.ExternalSessionID,
		"server_id":   ev.ServerID,
		"server_type": ev.ServerType,
	}
}

func guardNotificationVars(payload guardViolationPayload) map[string]string {
	return map[string]string{
		"user":      payload.User,
		"summary":   payload.Summary,
		"rule_type": payload.RuleType,
	}
}

func requestReadyNotificationVars(payload requestReadyPayload) map[string]string {
	title := firstNonEmptyStr(payload.Title, payload.ItemID, payload.RequestID)
	return map[string]string{
		"request_id":   payload.RequestID,
		"requested_by": payload.RequestedBy,
		"title":        title,
		"year":         payload.Year,
		"item_type":    payload.ItemType,
		"tmdb_id":      payload.TMDBID,
		"item_id":      payload.ItemID,
	}
}

func (m *Module) dispatchNotificationRule(ctx context.Context, rule NotificationRule, vars map[string]string) {
	title := renderNotificationTemplate(rule.TitleTemplate, vars)
	message := renderNotificationTemplate(rule.MessageTemplate, vars)
	if title == "" {
		title = rule.Name
	}
	if message == "" {
		message = rule.Name
	}
	fields := map[string]string{
		"event":   rule.EventType,
		"rule_id": rule.ID,
	}
	for k, v := range vars {
		fields[k] = v
	}
	if m.deliverNotificationToDestinations(ctx, rule.EventType, rule.DestinationIDs, title, message, rule.Severity, fields) {
		return
	}
	m.postNotification(ctx, title, message, rule.Severity, fields)
}

func renderNotificationTemplate(tmpl string, vars map[string]string) string {
	out := tmpl
	for key, val := range vars {
		out = strings.ReplaceAll(out, "{"+key+"}", val)
	}
	return out
}

func (m *Module) fireLegacyPlaybackNotification(ctx context.Context, eventType string, ev SessionEvent) {
	switch eventType {
	case playbackevents.EventPlaybackStarted:
		if m.getNotifyOnSessionStart() {
			m.notifySessionStartLegacy(ctx, ev)
		}
	case playbackevents.EventPlaybackStopped:
		if m.getNotifyOnSessionStop() {
			m.notifySessionStopLegacy(ctx, ev)
		}
	}
}

func notificationRuleToMap(rule NotificationRule) map[string]any {
	return map[string]any{
		"id":               rule.ID,
		"name":             rule.Name,
		"enabled":          rule.Enabled,
		"event_type":       rule.EventType,
		"title_template":   rule.TitleTemplate,
		"message_template": rule.MessageTemplate,
		"severity":         rule.Severity,
		"filters":          rule.Filters,
		"destination_ids":  rule.DestinationIDs,
		"created_at":       rule.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at":       rule.UpdatedAt.UTC().Format(time.RFC3339),
	}
}
