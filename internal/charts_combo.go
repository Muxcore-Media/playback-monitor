package internal

import (
	"context"
	"fmt"
	"strings"
)

func (m *Module) playsBySourceResolution(ctx context.Context, days, limit int) ([]chartBucketRow, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 20 {
		limit = 20
	}
	since, _ := m.chartSince(days)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT COALESCE(NULLIF(li.video_resolution, ''), 'unknown') AS resolution, COUNT(1)
		FROM sessions s
		LEFT JOIN library_items li ON li.server_id = s.server_id AND li.item_id = s.item_id
			AND `+activeLibraryItemsSQL("li")+`
		WHERE s.state = 'stopped' AND s.started_at >= ?
		GROUP BY resolution
		ORDER BY COUNT(1) DESC
		LIMIT ?`, since, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanChartBucketRows(rows)
}

func (m *Module) playsByPlatformResolution(ctx context.Context, days, limit int) ([]chartBucketRow, error) {
	if limit <= 0 {
		limit = 15
	}
	if limit > 30 {
		limit = 30
	}
	since, _ := m.chartSince(days)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT
			COALESCE(NULLIF(platform, ''), 'unknown') AS platform,
			COALESCE(NULLIF(stream_resolution, ''), 'unknown') AS resolution,
			COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY platform, resolution
		ORDER BY COUNT(1) DESC
		LIMIT ?`, since, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]chartBucketRow, 0)
	for rows.Next() {
		var platform, resolution string
		var count int
		if err := rows.Scan(&platform, &resolution, &count); err != nil {
			return nil, err
		}
		label := strings.TrimSpace(platform)
		if res := strings.TrimSpace(resolution); res != "" && res != "unknown" {
			label += " · " + res
		}
		out = append(out, chartBucketRow{Key: platform + "|" + resolution, Label: label, Count: count})
	}
	return out, rows.Err()
}
