package internal

import (
	"context"
	"fmt"
	"strings"
)

type libraryCatalogRollup struct {
	ServerID       string
	ServerType     string
	LibraryName    string
	ItemCount      int
	MovieCount     int
	EpisodeCount   int
	ShowCount      int
	TrackCount     int
	TotalFileSize  int64
	Resolutions    map[string]int
}

func activeLibraryItemsSQL(alias string) string {
	if alias != "" {
		alias += "."
	}
	return fmt.Sprintf(`(%[1]sremoved_at IS NULL OR %[1]sremoved_at = '')`, alias)
}

func (m *Module) listLibraryCatalogRollups(ctx context.Context, serverID string) ([]libraryCatalogRollup, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	countQuery := `
		SELECT li.server_id,
			COALESCE(NULLIF(s.type, ''), '') AS server_type,
			COALESCE(NULLIF(li.library_name, ''), '(unknown)') AS library_name,
			COUNT(1),
			SUM(CASE WHEN lower(li.media_type) IN ('movie', 'film') THEN 1 ELSE 0 END),
			SUM(CASE WHEN lower(li.media_type) = 'episode' THEN 1 ELSE 0 END),
			SUM(CASE WHEN lower(li.media_type) IN ('show', 'series', 'tv') THEN 1 ELSE 0 END),
			SUM(CASE WHEN lower(li.media_type) IN ('track', 'audio', 'music') THEN 1 ELSE 0 END),
			COALESCE(SUM(li.file_size_bytes), 0)
		FROM library_items li
		LEFT JOIN servers s ON s.id = li.server_id
		WHERE ` + activeLibraryItemsSQL("li") + `
			AND lower(li.media_type) != 'season'`
	args := []any{}
	if strings.TrimSpace(serverID) != "" {
		countQuery += ` AND li.server_id = ?`
		args = append(args, serverID)
	}
	countQuery += `
		GROUP BY li.server_id, s.type, li.library_name
		ORDER BY li.server_id, li.library_name`

	rows, err := m.queryRows(ctx, countQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byKey := make(map[string]*libraryCatalogRollup)
	order := make([]string, 0)
	for rows.Next() {
		var row libraryCatalogRollup
		if err := rows.Scan(&row.ServerID, &row.ServerType, &row.LibraryName,
			&row.ItemCount, &row.MovieCount, &row.EpisodeCount, &row.ShowCount, &row.TrackCount, &row.TotalFileSize); err != nil {
			return nil, err
		}
		row.Resolutions = map[string]int{}
		key := row.ServerID + "\x00" + row.LibraryName
		byKey[key] = &row
		order = append(order, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	resQuery := `
		SELECT li.server_id,
			COALESCE(NULLIF(li.library_name, ''), '(unknown)') AS library_name,
			COALESCE(NULLIF(li.video_resolution, ''), 'unknown') AS resolution,
			COUNT(1)
		FROM library_items li
		WHERE ` + activeLibraryItemsSQL("li") + `
			AND lower(li.media_type) != 'season'`
	resArgs := []any{}
	if strings.TrimSpace(serverID) != "" {
		resQuery += ` AND li.server_id = ?`
		resArgs = append(resArgs, serverID)
	}
	resQuery += `
		GROUP BY li.server_id, li.library_name, resolution
		ORDER BY li.server_id, li.library_name, resolution`

	resRows, err := m.queryRows(ctx, resQuery, resArgs...)
	if err != nil {
		return nil, err
	}
	defer resRows.Close()

	for resRows.Next() {
		var serverID, libraryName, resolution string
		var count int
		if err := resRows.Scan(&serverID, &libraryName, &resolution, &count); err != nil {
			return nil, err
		}
		key := serverID + "\x00" + libraryName
		row, ok := byKey[key]
		if !ok {
			row = &libraryCatalogRollup{
				ServerID:    serverID,
				LibraryName: libraryName,
				Resolutions: map[string]int{},
			}
			byKey[key] = row
			order = append(order, key)
		}
		if row.Resolutions == nil {
			row.Resolutions = map[string]int{}
		}
		row.Resolutions[resolution] = count
	}
	if err := resRows.Err(); err != nil {
		return nil, err
	}

	out := make([]libraryCatalogRollup, 0, len(order))
	for _, key := range order {
		if row := byKey[key]; row != nil {
			out = append(out, *row)
		}
	}
	return out, nil
}
