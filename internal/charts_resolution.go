package internal

import (
	"context"
	"fmt"

	playbackv1 "github.com/Muxcore-Media/playback-contract/proto/playbackv1"
)

func normalizeStreamResolution(height, width int, label string) string {
	return playbackv1.NormalizeStreamResolution(height, width, label)
}

func (m *Module) playsByStreamResolution(ctx context.Context, days, limit int) ([]chartBucketRow, error) {
	if days <= 0 {
		days = 30
	}
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
		SELECT COALESCE(NULLIF(stream_resolution, ''), 'unknown') AS resolution, COUNT(1)
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?
		GROUP BY resolution
		ORDER BY COUNT(1) DESC
		LIMIT ?`, since, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanChartBucketRows(rows)
}
