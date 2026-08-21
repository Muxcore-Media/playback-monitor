package internal

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"
)

type libraryStatRow struct {
	ServerID      string
	LibraryName   string
	PlayCount     int
	WatchMinutes  float64
}

type topContentRow struct {
	Title        string
	MediaType    string
	PlayCount    int
	WatchMinutes float64
}

func deriveLibraryName(mediaPath string) string {
	p := strings.TrimSpace(strings.ReplaceAll(mediaPath, "\\", "/"))
	if p == "" {
		return ""
	}
	p = strings.Trim(p, "/")
	if p == "" {
		return ""
	}
	parts := strings.Split(p, "/")
	skipRoots := map[string]bool{
		"media": true, "data": true, "mnt": true, "volume": true, "var": true, "srv": true,
	}
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		lower := strings.ToLower(part)
		if skipRoots[lower] {
			continue
		}
		return part
	}
	if len(parts) > 0 {
		return path.Base(parts[0])
	}
	return ""
}

func sessionPathFields(ev SessionEvent) (mediaPath, libraryName string) {
	mediaPath = strings.TrimSpace(ev.MediaPath)
	libraryName = strings.TrimSpace(ev.LibraryName)
	if libraryName == "" {
		libraryName = deriveLibraryName(mediaPath)
	}
	return mediaPath, libraryName
}

func (m *Module) listLibraryStats(ctx context.Context, days int, serverID string, limit int) ([]libraryStatRow, error) {
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

	query := `
		SELECT server_id,
			COALESCE(NULLIF(library_name, ''), '(unknown)') AS library_name,
			COUNT(1),
			COALESCE(SUM(position_seconds), 0) / 60.0
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ?`
	args := []any{since}
	if strings.TrimSpace(serverID) != "" {
		query += ` AND server_id = ?`
		args = append(args, serverID)
	}
	query += `
		GROUP BY server_id, library_name
		ORDER BY COUNT(1) DESC
		LIMIT ?`
	args = append(args, limit)

	rows, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]libraryStatRow, 0)
	for rows.Next() {
		var row libraryStatRow
		if err := rows.Scan(&row.ServerID, &row.LibraryName, &row.PlayCount, &row.WatchMinutes); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (m *Module) listTopContent(ctx context.Context, days int, serverID string, limit int) (movies, shows, other []topContentRow, err error) {
	if days <= 0 {
		days = 30
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	since := time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, nil, nil, fmt.Errorf("db not initialized")
	}

	query := `
		SELECT title, media_type, COUNT(1), COALESCE(SUM(position_seconds), 0) / 60.0
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ? AND title != ''`
	args := []any{since}
	if strings.TrimSpace(serverID) != "" {
		query += ` AND server_id = ?`
		args = append(args, serverID)
	}
	query += `
		GROUP BY title, media_type
		ORDER BY COUNT(1) DESC
		LIMIT ?`
	args = append(args, limit*3)

	rows, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	movies = make([]topContentRow, 0)
	shows = make([]topContentRow, 0)
	other = make([]topContentRow, 0)
	for rows.Next() {
		var row topContentRow
		if err := rows.Scan(&row.Title, &row.MediaType, &row.PlayCount, &row.WatchMinutes); err != nil {
			return nil, nil, nil, err
		}
		switch classifyMediaType(row.MediaType) {
		case "movie":
			if len(movies) < limit {
				movies = append(movies, row)
			}
		case "show":
			if len(shows) < limit {
				shows = append(shows, row)
			}
		default:
			if len(other) < limit {
				other = append(other, row)
			}
		}
	}
	return movies, shows, other, rows.Err()
}

func classifyMediaType(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "movie", "film":
		return "movie"
	case "episode", "series", "tv", "show":
		return "show"
	case "season":
		return "season"
	default:
		return "other"
	}
}
