package internal

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type libraryDuplicateCopy struct {
	ServerID      string
	ItemID        string
	Title         string
	LibraryName   string
	MediaPath     string
	FileSizeBytes int64
}

type libraryDuplicateGroup struct {
	GroupKey  string
	Title     string
	CopyCount int
	Copies    []libraryDuplicateCopy
}

func normalizeDuplicateTitle(title string) string {
	title = strings.ToLower(strings.TrimSpace(title))
	if title == "" {
		return ""
	}
	return strings.Join(strings.Fields(title), " ")
}

func duplicateGroupKey(muxcoreID, title string) string {
	if id := strings.TrimSpace(muxcoreID); id != "" {
		return "mux:" + id
	}
	if t := normalizeDuplicateTitle(title); t != "" {
		return "title:" + t
	}
	return ""
}

func (m *Module) listLibraryDuplicates(ctx context.Context, serverID string, limit int) ([]libraryDuplicateGroup, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	query := `
		SELECT server_id, item_id, muxcore_id, title, library_name, media_path, file_size_bytes
		FROM library_items
		WHERE ` + activeLibraryItemsSQL("")
	args := []any{}
	if strings.TrimSpace(serverID) != "" {
		query += ` AND server_id = ?`
		args = append(args, serverID)
	}
	query += ` ORDER BY title ASC, server_id ASC, item_id ASC`

	rows, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	grouped := make(map[string]*libraryDuplicateGroup)
	for rows.Next() {
		var copy libraryDuplicateCopy
		var muxcoreID string
		if err := rows.Scan(&copy.ServerID, &copy.ItemID, &muxcoreID, &copy.Title,
			&copy.LibraryName, &copy.MediaPath, &copy.FileSizeBytes); err != nil {
			return nil, err
		}
		key := duplicateGroupKey(muxcoreID, copy.Title)
		if key == "" {
			continue
		}
		g, ok := grouped[key]
		if !ok {
			title := strings.TrimSpace(copy.Title)
			if title == "" {
				title = muxcoreID
			}
			g = &libraryDuplicateGroup{GroupKey: key, Title: title}
			grouped[key] = g
		}
		g.Copies = append(g.Copies, copy)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]libraryDuplicateGroup, 0)
	for _, g := range grouped {
		if len(g.Copies) < 2 {
			continue
		}
		g.CopyCount = len(g.Copies)
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CopyCount != out[j].CopyCount {
			return out[i].CopyCount > out[j].CopyCount
		}
		return out[i].Title < out[j].Title
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
