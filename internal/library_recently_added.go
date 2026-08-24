package internal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type recentlyAddedItem struct {
	AddedAt       time.Time
	RemovedAt     *time.Time
	ServerID      string
	ServerType    string
	ItemID        string
	MuxcoreID     string
	Title         string
	MediaType     string
	LibraryName   string
	FileSizeBytes int64
}

type recentlyAddedCursor struct {
	AddedAt  string `json:"added_at"`
	ServerID string `json:"server_id"`
	ItemID   string `json:"item_id"`
}

func encodeRecentlyAddedCursor(addedAt time.Time, serverID, itemID string) string {
	payload, _ := json.Marshal(recentlyAddedCursor{
		AddedAt:  addedAt.UTC().Format(time.RFC3339Nano),
		ServerID: serverID,
		ItemID:   itemID,
	})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeRecentlyAddedCursor(raw string) (time.Time, string, string, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, "", "", nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, "", "", fmt.Errorf("invalid cursor")
	}
	var cur recentlyAddedCursor
	if unmarshalErr := json.Unmarshal(data, &cur); unmarshalErr != nil {
		return time.Time{}, "", "", fmt.Errorf("invalid cursor")
	}
	if cur.ServerID == "" || cur.ItemID == "" || cur.AddedAt == "" {
		return time.Time{}, "", "", fmt.Errorf("invalid cursor")
	}
	added, err := time.Parse(time.RFC3339Nano, cur.AddedAt)
	if err != nil {
		added, err = time.Parse(time.RFC3339, cur.AddedAt)
	}
	if err != nil {
		return time.Time{}, "", "", fmt.Errorf("invalid cursor")
	}
	return added.UTC(), cur.ServerID, cur.ItemID, nil
}

func libraryAddedExpr() string {
	return `COALESCE(NULLIF(li.catalog_added_at, ''), li.updated_at)`
}

func (m *Module) listRecentlyAddedLibraryItems(
	ctx context.Context,
	serverID, libraryID, mediaType string,
	includeRemoved bool,
	pageSize int,
	cursorRaw string,
) ([]recentlyAddedItem, string, error) {
	if pageSize <= 0 {
		pageSize = 25
	}
	if pageSize > 100 {
		pageSize = 100
	}
	cursorAdded, cursorServer, cursorItem, err := decodeRecentlyAddedCursor(cursorRaw)
	if err != nil {
		return nil, "", err
	}

	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, "", fmt.Errorf("db not initialized")
	}

	where := []string{`(` + libraryAddedExpr() + `) != ''`}
	args := []any{}
	if !includeRemoved {
		where = append(where, `(li.removed_at IS NULL OR li.removed_at = '')`)
	}
	if strings.TrimSpace(serverID) != "" {
		where = append(where, `li.server_id = ?`)
		args = append(args, serverID)
	}
	if strings.TrimSpace(libraryID) != "" {
		where = append(where, `li.library_name = ?`)
		args = append(args, libraryID)
	}
	if strings.TrimSpace(mediaType) != "" {
		where = append(where, `lower(li.media_type) = lower(?)`)
		args = append(args, mediaType)
	}
	if !cursorAdded.IsZero() {
		addedExpr := libraryAddedExpr()
		where = append(where, `(
			`+addedExpr+` < ? OR
			(`+addedExpr+` = ? AND (li.server_id < ? OR (li.server_id = ? AND li.item_id < ?)))
		)`)
		ts := cursorAdded.Format(time.RFC3339Nano)
		args = append(args, ts, ts, cursorServer, cursorServer, cursorItem)
	}

	query := `
		SELECT li.server_id, COALESCE(s.type, li.server_id), li.item_id, li.muxcore_id, li.title,
			li.media_type, li.library_name, li.file_size_bytes,
			` + libraryAddedExpr() + ` AS added_at, li.removed_at
		FROM library_items li
		LEFT JOIN servers s ON s.id = li.server_id
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY added_at DESC, li.server_id DESC, li.item_id DESC
		LIMIT ?`
	listArgs := append(append([]any{}, args...), pageSize+1)
	rows, err := m.queryRows(ctx, query, listArgs...)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()

	out := make([]recentlyAddedItem, 0)
	for rows.Next() {
		var item recentlyAddedItem
		var addedAt, removedAt string
		if err := rows.Scan(
			&item.ServerID, &item.ServerType, &item.ItemID, &item.MuxcoreID, &item.Title,
			&item.MediaType, &item.LibraryName, &item.FileSizeBytes, &addedAt, &removedAt,
		); err != nil {
			return nil, "", err
		}
		item.AddedAt = parseTime(addedAt)
		if strings.TrimSpace(removedAt) != "" {
			t := parseTime(removedAt)
			item.RemovedAt = &t
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(out) > pageSize {
		last := out[pageSize-1]
		nextCursor = encodeRecentlyAddedCursor(last.AddedAt, last.ServerID, last.ItemID)
		out = out[:pageSize]
	}
	return out, nextCursor, nil
}

func recentlyAddedItemJSON(item recentlyAddedItem) map[string]any {
	removedAt := any(nil)
	if item.RemovedAt != nil && !item.RemovedAt.IsZero() {
		removedAt = item.RemovedAt.UTC().Format(time.RFC3339)
	}
	addedAt := ""
	if !item.AddedAt.IsZero() {
		addedAt = item.AddedAt.UTC().Format(time.RFC3339)
	}
	id := item.ServerID + ":" + item.ItemID
	if item.MuxcoreID != "" {
		id = item.MuxcoreID
	}
	return map[string]any{
		"id":              id,
		"server_id":       item.ServerID,
		"server_type":     item.ServerType,
		"library_id":      item.LibraryName,
		"item_id":         item.ItemID,
		"muxcore_id":      item.MuxcoreID,
		"media_type":      item.MediaType,
		"title":           item.Title,
		"file_size_bytes": item.FileSizeBytes,
		"added_at":        addedAt,
		"removed_at":      removedAt,
	}
}
