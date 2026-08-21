package internal

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

type staleLibraryItem struct {
	ServerID      string
	ItemID        string
	Title         string
	MediaType     string
	LibraryName   string
	FileSizeBytes int64
	LastWatched   *time.Time
	WatchCount    int
	Category      string
	DaysStale     int
}

func (m *Module) listStaleLibraryItems(ctx context.Context, serverID string, staleDays, limit int) ([]staleLibraryItem, int, int, error) {
	if staleDays <= 0 {
		staleDays = 90
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -staleDays)

	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, 0, 0, fmt.Errorf("db not initialized")
	}

	query := `
		SELECT li.server_id, li.item_id, li.title, li.media_type, li.library_name,
			li.file_size_bytes, li.updated_at,
			MAX(CASE WHEN s.state = 'stopped' AND s.stopped_at != '' THEN s.stopped_at END) AS last_watched,
			COUNT(s.id) AS watch_count
		FROM library_items li
		LEFT JOIN sessions s ON s.server_id = li.server_id AND s.item_id = li.item_id
		WHERE ` + activeLibraryItemsSQL("li")
	args := []any{}
	if strings.TrimSpace(serverID) != "" {
		query += ` AND li.server_id = ?`
		args = append(args, serverID)
	}
	query += `
		GROUP BY li.server_id, li.item_id, li.title, li.media_type, li.library_name, li.file_size_bytes, li.updated_at`

	rows, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()

	now := time.Now().UTC()
	out := make([]staleLibraryItem, 0)
	neverCount, staleCount := 0, 0
	for rows.Next() {
		var item staleLibraryItem
		var updatedAt string
		var lastWatched sql.NullString
		if err := rows.Scan(&item.ServerID, &item.ItemID, &item.Title, &item.MediaType, &item.LibraryName,
			&item.FileSizeBytes, &updatedAt, &lastWatched, &item.WatchCount); err != nil {
			return nil, 0, 0, err
		}
		addedAt, _ := time.Parse(time.RFC3339, updatedAt)
		if lastWatched.Valid && strings.TrimSpace(lastWatched.String) != "" {
			if t, err := time.Parse(time.RFC3339, lastWatched.String); err == nil {
				item.LastWatched = &t
			}
		}
		if item.LastWatched == nil {
			item.Category = "never_watched"
			item.DaysStale = int(now.Sub(addedAt).Hours() / 24)
			if item.DaysStale < 0 {
				item.DaysStale = 0
			}
			neverCount++
			out = append(out, item)
			continue
		}
		if item.LastWatched.Before(cutoff) {
			item.Category = "stale"
			item.DaysStale = int(now.Sub(*item.LastWatched).Hours() / 24)
			staleCount++
			out = append(out, item)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].DaysStale > out[j].DaysStale
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, neverCount, staleCount, nil
}
