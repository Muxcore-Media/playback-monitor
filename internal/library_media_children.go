package internal

import (
	"context"
	"fmt"
	"strings"
)

type mediaChildRow struct {
	ServerID        string
	ItemID          string
	MuxcoreID       string
	Title           string
	MediaType       string
	LibraryName     string
	VideoResolution string
	FileSizeBytes   int64
}

func mediaHasChildren(mediaType string) bool {
	switch classifyMediaType(mediaType) {
	case "show", "season":
		return true
	default:
		return false
	}
}

func (m *Module) listMediaChildren(ctx context.Context, rec *resolvedMedia) ([]mediaChildRow, error) {
	if rec == nil || strings.TrimSpace(rec.ItemID) == "" {
		return nil, fmt.Errorf("media ref required")
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	query := `
		SELECT server_id, item_id, muxcore_id, title, media_type, library_name, video_resolution, file_size_bytes
		FROM library_items
		WHERE parent_id = ? AND ` + activeLibraryItemsSQL("")
	args := []any{rec.ItemID}
	if strings.TrimSpace(rec.ServerID) != "" {
		query += ` AND server_id = ?`
		args = append(args, rec.ServerID)
	}
	query += ` ORDER BY title ASC, item_id ASC`

	rows, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]mediaChildRow, 0)
	for rows.Next() {
		var row mediaChildRow
		if err := rows.Scan(&row.ServerID, &row.ItemID, &row.MuxcoreID, &row.Title, &row.MediaType,
			&row.LibraryName, &row.VideoResolution, &row.FileSizeBytes); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (m *Module) countMediaDescendants(ctx context.Context, rec *resolvedMedia) (seasons, episodes int, err error) {
	if rec == nil || !mediaHasChildren(rec.MediaType) {
		return 0, 0, nil
	}
	children, err := m.listMediaChildren(ctx, rec)
	if err != nil {
		return 0, 0, err
	}
	for _, child := range children {
		switch classifyMediaType(child.MediaType) {
		case "season":
			seasons++
			grandchildren, err := m.listMediaChildren(ctx, &resolvedMedia{
				ServerID: child.ServerID,
				ItemID:   child.ItemID,
			})
			if err != nil {
				return seasons, episodes, err
			}
			episodes += len(grandchildren)
		case "episode":
			episodes++
		}
	}
	return seasons, episodes, nil
}

func publicMediaChildRow(row mediaChildRow) map[string]any {
	id := row.ServerID + ":" + row.ItemID
	if row.MuxcoreID != "" {
		id = "muxcore:" + row.MuxcoreID
	}
	return map[string]any{
		"id":               id,
		"server_id":        row.ServerID,
		"item_id":          row.ItemID,
		"muxcore_id":       row.MuxcoreID,
		"title":            row.Title,
		"media_type":       row.MediaType,
		"library_id":       row.LibraryName,
		"video_resolution": row.VideoResolution,
		"file_size_bytes":  row.FileSizeBytes,
	}
}
