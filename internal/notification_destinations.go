package internal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

var destinationHTTPClient = &http.Client{Timeout: 15 * time.Second}

type NotificationDestination struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Config    map[string]string
	ID        string
	Name      string
	Type      string
	Events    []string
	Enabled   bool
}

func isAllowedDestinationType(t string) bool {
	switch strings.TrimSpace(t) {
	case "discord", "slack", "webhook", "apprise":
		return true
	default:
		return false
	}
}

func (m *Module) listNotificationDestinations(ctx context.Context) ([]NotificationDestination, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT id, name, type, enabled, config_json, events_json, created_at, updated_at
		FROM notification_destinations
		ORDER BY name ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanNotificationDestinations(rows)
}

func scanNotificationDestinations(rows *sql.Rows) ([]NotificationDestination, error) {
	out := make([]NotificationDestination, 0)
	for rows.Next() {
		var dest NotificationDestination
		var enabled int
		var configJSON, eventsJSON, createdAt, updatedAt string
		if err := rows.Scan(
			&dest.ID, &dest.Name, &dest.Type, &enabled,
			&configJSON, &eventsJSON, &createdAt, &updatedAt,
		); err != nil {
			return nil, err
		}
		dest.Enabled = enabled == 1
		dest.CreatedAt = parseTime(createdAt)
		dest.UpdatedAt = parseTime(updatedAt)
		if configJSON != "" {
			_ = json.Unmarshal([]byte(configJSON), &dest.Config)
		}
		if dest.Config == nil {
			dest.Config = map[string]string{}
		}
		if eventsJSON != "" {
			_ = json.Unmarshal([]byte(eventsJSON), &dest.Events)
		}
		out = append(out, dest)
	}
	return out, rows.Err()
}

func (m *Module) getNotificationDestination(ctx context.Context, id string) (NotificationDestination, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return NotificationDestination{}, fmt.Errorf("db not initialized")
	}
	var dest NotificationDestination
	var enabled int
	var configJSON, eventsJSON, createdAt, updatedAt string
	err := m.queryRow(ctx, `
		SELECT id, name, type, enabled, config_json, events_json, created_at, updated_at
		FROM notification_destinations WHERE id = ?`, id).Scan(
		&dest.ID, &dest.Name, &dest.Type, &enabled,
		&configJSON, &eventsJSON, &createdAt, &updatedAt,
	)
	if err != nil {
		return NotificationDestination{}, err
	}
	dest.Enabled = enabled == 1
	dest.CreatedAt = parseTime(createdAt)
	dest.UpdatedAt = parseTime(updatedAt)
	if configJSON != "" {
		_ = json.Unmarshal([]byte(configJSON), &dest.Config)
	}
	if dest.Config == nil {
		dest.Config = map[string]string{}
	}
	if eventsJSON != "" {
		_ = json.Unmarshal([]byte(eventsJSON), &dest.Events)
	}
	return dest, nil
}

func (m *Module) upsertNotificationDestination(ctx context.Context, dest NotificationDestination) (NotificationDestination, error) {
	if strings.TrimSpace(dest.Name) == "" {
		return NotificationDestination{}, fmt.Errorf("name required")
	}
	if !isAllowedDestinationType(dest.Type) {
		return NotificationDestination{}, fmt.Errorf("unsupported type")
	}
	if len(dest.Events) == 0 {
		return NotificationDestination{}, fmt.Errorf("events required")
	}
	for _, ev := range dest.Events {
		if !isAllowedNotificationEventType(ev) {
			return NotificationDestination{}, fmt.Errorf("unsupported event %q", ev)
		}
	}
	if err := validateDestinationConfig(dest.Type, dest.Config); err != nil {
		return NotificationDestination{}, err
	}
	if dest.ID == "" {
		dest.ID = uuid.NewString()
	}
	now := time.Now().UTC().Format(time.RFC3339)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return NotificationDestination{}, fmt.Errorf("db not initialized")
	}
	configJSON, err := json.Marshal(dest.Config)
	if err != nil {
		return NotificationDestination{}, err
	}
	eventsJSON, err := json.Marshal(dest.Events)
	if err != nil {
		return NotificationDestination{}, err
	}
	var existingCreated string
	_ = m.queryRow(ctx, `SELECT created_at FROM notification_destinations WHERE id = ?`, dest.ID).Scan(&existingCreated)
	createdAt := now
	if existingCreated != "" {
		createdAt = existingCreated
	}
	_, err = m.exec(ctx, `
		INSERT INTO notification_destinations(
			id, name, type, enabled, config_json, events_json, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			type = excluded.type,
			enabled = excluded.enabled,
			config_json = excluded.config_json,
			events_json = excluded.events_json,
			updated_at = excluded.updated_at`,
		dest.ID, dest.Name, dest.Type, boolToInt(dest.Enabled), string(configJSON), string(eventsJSON), createdAt, now,
	)
	if err != nil {
		return NotificationDestination{}, err
	}
	return m.getNotificationDestination(ctx, dest.ID)
}

func (m *Module) deleteNotificationDestination(ctx context.Context, id string) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := m.txExec(ctx, tx, `DELETE FROM notification_destinations WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	if err := m.removeDestinationFromRulesTx(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *Module) removeDestinationFromRulesTx(ctx context.Context, tx *sql.Tx, destID string) error {
	rows, err := m.txQueryRows(ctx, tx, `SELECT id, destination_ids_json FROM notification_rules`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	type row struct {
		id  string
		ids string
	}
	var updates []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.ids); err != nil {
			return err
		}
		updates = append(updates, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, r := range updates {
		var ids []string
		if r.ids != "" {
			_ = json.Unmarshal([]byte(r.ids), &ids)
		}
		next := make([]string, 0, len(ids))
		changed := false
		for _, id := range ids {
			if id == destID {
				changed = true
				continue
			}
			next = append(next, id)
		}
		if !changed {
			continue
		}
		b, _ := json.Marshal(next)
		if _, err := m.txExec(ctx, tx, `UPDATE notification_rules SET destination_ids_json = ? WHERE id = ?`, string(b), r.id); err != nil {
			return err
		}
	}
	return nil
}

func validateDestinationConfig(destType string, config map[string]string) error {
	switch destType {
	case "discord", "slack", "webhook":
		urlVal := strings.TrimSpace(config["webhook_url"])
		if urlVal == "" {
			return fmt.Errorf("webhook_url required")
		}
		return assertSafeWebhookURL(urlVal)
	case "apprise":
		if strings.TrimSpace(config["urls"]) == "" {
			return fmt.Errorf("urls required")
		}
		return nil
	default:
		return fmt.Errorf("unsupported type")
	}
}

func assertSafeWebhookURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid webhook_url")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("webhook_url must be http or https")
	}
	if envTruthy(os.Getenv("PLAYBACK_MONITOR_ALLOW_LOCAL_WEBHOOKS")) {
		return nil
	}
	host := u.Hostname()
	if strings.EqualFold(host, "localhost") {
		return fmt.Errorf("webhook_url blocked")
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
			return fmt.Errorf("webhook_url blocked")
		}
	}
	return nil
}

func destinationSupportsEvent(dest NotificationDestination, eventType string) bool {
	if !dest.Enabled {
		return false
	}
	for _, ev := range dest.Events {
		if ev == eventType {
			return true
		}
	}
	return false
}

func (m *Module) deliverNotificationToDestinations(
	ctx context.Context,
	eventType string,
	destinationIDs []string,
	title, message, severity string,
	fields map[string]string,
) bool {
	if len(destinationIDs) == 0 {
		return false
	}
	delivered := false
	for _, id := range destinationIDs {
		dest, err := m.getNotificationDestination(ctx, id)
		if err != nil {
			continue
		}
		if !destinationSupportsEvent(dest, eventType) {
			continue
		}
		if err := m.deliverToDestination(ctx, dest, title, message, severity, fields); err == nil {
			delivered = true
		}
	}
	return delivered
}

func (m *Module) deliverToDestination(
	ctx context.Context,
	dest NotificationDestination,
	title, message, severity string,
	fields map[string]string,
) error {
	switch dest.Type {
	case "discord":
		return m.postDiscordWebhook(ctx, dest.Config["webhook_url"], title, message, severity, fields)
	case "slack":
		return m.postSlackWebhook(ctx, dest.Config["webhook_url"], title, message, severity)
	case "webhook":
		return m.postGenericWebhook(ctx, dest.Config["webhook_url"], title, message, severity, fields)
	case "apprise":
		return m.postAppriseURLs(ctx, dest.Config["urls"], title, message, severity)
	default:
		return fmt.Errorf("unsupported destination type")
	}
}

func (m *Module) postDiscordWebhook(ctx context.Context, webhookURL, title, message, severity string, fields map[string]string) error {
	color := 3447003
	switch strings.ToLower(severity) {
	case "warning":
		color = 16776960
	case "error":
		color = 15158332
	case "success":
		color = 3066993
	}
	embedFields := make([]map[string]any, 0, len(fields))
	for k, v := range fields {
		embedFields = append(embedFields, map[string]any{"name": k, "value": v, "inline": true})
	}
	payload, err := json.Marshal(map[string]any{
		"embeds": []map[string]any{{
			"title":       title,
			"description": message,
			"color":       color,
			"fields":      embedFields,
		}},
	})
	if err != nil {
		return err
	}
	return m.postWebhookJSON(ctx, webhookURL, payload)
}

func (m *Module) postSlackWebhook(ctx context.Context, webhookURL, title, message, severity string) error {
	payload, err := json.Marshal(map[string]any{
		"text": fmt.Sprintf("*%s* [%s]\n%s", title, severity, message),
	})
	if err != nil {
		return err
	}
	return m.postWebhookJSON(ctx, webhookURL, payload)
}

func (m *Module) postGenericWebhook(ctx context.Context, webhookURL, title, message, severity string, fields map[string]string) error {
	body := map[string]any{
		"title":    title,
		"message":  message,
		"severity": severity,
		"fields":   fields,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return m.postWebhookJSON(ctx, webhookURL, payload)
}

func (m *Module) postAppriseURLs(ctx context.Context, urls, title, message, severity string) error {
	base := strings.TrimSpace(os.Getenv("PLAYBACK_MONITOR_APPRISE_URL"))
	if base == "" {
		base = strings.TrimSpace(os.Getenv("APPRISE_URL"))
	}
	if base == "" {
		base = "http://localhost:8000"
	}
	base = strings.TrimRight(base, "/")
	payload, err := json.Marshal(map[string]any{
		"urls":  urls,
		"title": title,
		"body":  message,
		"type":  appriseNotifyType(severity),
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/notify", bytes.NewReader(payload)) //nolint:gosec // destination base URL is operator-configured
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(os.Getenv("PLAYBACK_MONITOR_APPRISE_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if token := strings.TrimSpace(os.Getenv("APPRISE_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := destinationHTTPClient.Do(req) //nolint:gosec // destination base URL is operator-configured
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("apprise returned %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

func appriseNotifyType(severity string) string {
	switch strings.ToLower(severity) {
	case "error":
		return "failure"
	case "warning":
		return "warning"
	case "success":
		return "success"
	default:
		return "info"
	}
}

func (m *Module) postWebhookJSON(ctx context.Context, webhookURL string, payload []byte) error {
	if err := assertSafeWebhookURL(webhookURL); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := destinationHTTPClient.Do(req) //nolint:gosec // destination base URL is operator-configured
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("webhook returned %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

func (m *Module) testNotificationDestination(ctx context.Context, id string) error {
	dest, err := m.getNotificationDestination(ctx, id)
	if err != nil {
		return err
	}
	return m.deliverToDestination(ctx, dest, "MuxCore test notification", "This is a test message from playback-monitor.", "info", map[string]string{
		"destination_id": dest.ID,
		"destination":    dest.Name,
	})
}

func notificationDestinationToMap(dest NotificationDestination) map[string]any {
	config := map[string]any{}
	for k, v := range dest.Config {
		if strings.Contains(k, "url") || strings.Contains(k, "token") || k == "urls" {
			if v != "" {
				config[k] = "********"
			} else {
				config[k] = ""
			}
		} else {
			config[k] = v
		}
	}
	return map[string]any{
		"id":         dest.ID,
		"name":       dest.Name,
		"type":       dest.Type,
		"enabled":    dest.Enabled,
		"config":     config,
		"events":     dest.Events,
		"created_at": dest.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": dest.UpdatedAt.UTC().Format(time.RFC3339),
	}
}
