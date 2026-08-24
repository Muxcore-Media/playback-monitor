package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

type playsByDateRow struct {
	Date  string
	Count int
}

type chartBucketRow struct {
	Key   string
	Label string
	Count int
}

var dayOfWeekLabels = []string{
	"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
}

func (m *Module) chartSince(days int) (string, int) {
	if days <= 0 {
		days = 30
	}
	return time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339), days
}

func (m *Module) playsByDate(ctx context.Context, days int) ([]playsByDateRow, error) {
	since, _ := m.chartSince(days)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT substr(started_at, 1, 10) AS day, COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY day
		ORDER BY day ASC`, since)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]playsByDateRow, 0)
	for rows.Next() {
		var row playsByDateRow
		if err := rows.Scan(&row.Date, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (m *Module) playsByHour(ctx context.Context, days int) ([]chartBucketRow, error) {
	since, _ := m.chartSince(days)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	counts := make([]int, 24)
	rows, err := m.queryRows(ctx, `
		SELECT CAST(strftime('%H', started_at) AS INTEGER) AS hour, COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY hour`, since)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var hour, count int
		if err := rows.Scan(&hour, &count); err != nil {
			return nil, err
		}
		if hour >= 0 && hour < 24 {
			counts[hour] = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]chartBucketRow, 0, 24)
	for hour := 0; hour < 24; hour++ {
		out = append(out, chartBucketRow{
			Key:   strconv.Itoa(hour),
			Label: fmt.Sprintf("%02d:00", hour),
			Count: counts[hour],
		})
	}
	return out, nil
}

func (m *Module) playsByDayOfWeek(ctx context.Context, days int) ([]chartBucketRow, error) {
	since, _ := m.chartSince(days)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	counts := make([]int, 7)
	rows, err := m.queryRows(ctx, `
		SELECT CAST(strftime('%w', started_at) AS INTEGER) AS dow, COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY dow`, since)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var dow, count int
		if err := rows.Scan(&dow, &count); err != nil {
			return nil, err
		}
		if dow >= 0 && dow < 7 {
			counts[dow] = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]chartBucketRow, 0, 7)
	for dow := 0; dow < 7; dow++ {
		out = append(out, chartBucketRow{
			Key:   strconv.Itoa(dow),
			Label: dayOfWeekLabels[dow],
			Count: counts[dow],
		})
	}
	return out, nil
}

func (m *Module) playsByMonth(ctx context.Context, days int) ([]chartBucketRow, error) {
	if days <= 0 {
		days = 365
	}
	since := time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT substr(started_at, 1, 7) AS month, COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY month
		ORDER BY month ASC`, since)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]chartBucketRow, 0)
	for rows.Next() {
		var month string
		var count int
		if err := rows.Scan(&month, &count); err != nil {
			return nil, err
		}
		out = append(out, chartBucketRow{Key: month, Label: month, Count: count})
	}
	return out, rows.Err()
}

func (m *Module) playsByStreamType(ctx context.Context, days int) ([]chartBucketRow, error) {
	_, transcodes, err := m.streamAnalytics(ctx, days)
	if err != nil {
		return nil, err
	}
	out := make([]chartBucketRow, 0, len(transcodes))
	for _, row := range transcodes {
		out = append(out, chartBucketRow(row))
	}
	return out, nil
}

func (m *Module) playsByTopUsers(ctx context.Context, days, limit int) ([]chartBucketRow, error) {
	if days <= 0 {
		days = 30
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	since, _ := m.chartSince(days)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT COALESCE(NULLIF(user_name, ''), user_id, 'unknown') AS username, COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY username
		ORDER BY COUNT(1) DESC
		LIMIT ?`, since, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanChartBucketRows(rows)
}

func (m *Module) playsByTopPlatforms(ctx context.Context, days, limit int) ([]chartBucketRow, error) {
	if days <= 0 {
		days = 30
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	since, _ := m.chartSince(days)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT COALESCE(NULLIF(platform, ''), 'unknown') AS platform, COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY platform
		ORDER BY COUNT(1) DESC
		LIMIT ?`, since, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanChartBucketRows(rows)
}

func scanChartBucketRows(rows *sql.Rows) ([]chartBucketRow, error) {
	out := make([]chartBucketRow, 0)
	for rows.Next() {
		var label string
		var count int
		if err := rows.Scan(&label, &count); err != nil {
			return nil, err
		}
		out = append(out, chartBucketRow{
			Key:   label,
			Label: labelForBreakdownKey(label),
			Count: count,
		})
	}
	return out, rows.Err()
}
