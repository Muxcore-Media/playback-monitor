package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type breakdownRow struct {
	Key   string
	Label string
	Count int
}

type userWatchStat struct {
	Username     string
	PlayCount    int
	WatchMinutes float64
}

func (m *Module) streamAnalytics(ctx context.Context, days int) ([]breakdownRow, []breakdownRow, error) {
	if days <= 0 {
		days = 30
	}
	since := time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, nil, fmt.Errorf("db not initialized")
	}

	platforms, err := m.groupCount(db, ctx, `
		SELECT COALESCE(NULLIF(platform,''), 'unknown') AS k, COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY k
		ORDER BY COUNT(1) DESC
		LIMIT 20`, since)
	if err != nil {
		return nil, nil, err
	}

	transcodes, err := m.groupCount(db, ctx, `
		SELECT CASE WHEN is_transcode = 1 THEN 'transcode' ELSE 'direct' END AS k, COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY k
		ORDER BY COUNT(1) DESC`, since)
	if err != nil {
		return nil, nil, err
	}
	return platforms, transcodes, nil
}

func (m *Module) listUserWatchStats(ctx context.Context, days, limit int) ([]userWatchStat, error) {
	if days <= 0 {
		days = 30
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	since := time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT COALESCE(NULLIF(user_name,''), user_id, 'unknown') AS username,
			COUNT(1) AS plays,
			COALESCE(SUM(position_seconds), 0) / 60.0 AS minutes
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY username
		ORDER BY minutes DESC, plays DESC
		LIMIT ?`, since, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]userWatchStat, 0)
	for rows.Next() {
		var u userWatchStat
		if err := rows.Scan(&u.Username, &u.PlayCount, &u.WatchMinutes); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (m *Module) groupCount(db *sql.DB, ctx context.Context, query, since string) ([]breakdownRow, error) {
	rows, err := m.queryRows(ctx, query, since)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]breakdownRow, 0)
	for rows.Next() {
		var key string
		var count int
		if err := rows.Scan(&key, &count); err != nil {
			return nil, err
		}
		out = append(out, breakdownRow{
			Key:   key,
			Label: labelForBreakdownKey(key),
			Count: count,
		})
	}
	return out, rows.Err()
}

func labelForBreakdownKey(key string) string {
	switch strings.ToLower(key) {
	case "transcode":
		return "Transcode"
	case "direct":
		return "Direct play/stream"
	case "unknown":
		return "Unknown"
	default:
		return key
	}
}
