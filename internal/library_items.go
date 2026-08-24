package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type libraryItemRecord struct {
	UpdatedAt     time.Time
	ServerID      string
	ItemID        string
	MuxcoreID     string
	Title         string
	MediaType     string
	LibraryName   string
	MediaPath     string
	FileSizeBytes int64
}

func (m *Module) upsertLibraryItem(ctx context.Context, serverID string, ev SessionEvent) error {
	itemID := strings.TrimSpace(ev.ItemID)
	if itemID == "" {
		return nil
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	mediaPath, libraryName := sessionPathFields(ev)
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := m.exec(ctx, `
		INSERT INTO library_items(
			server_id, item_id, muxcore_id, title, media_type, library_name, media_path, imdb_id, tmdb_id, tvdb_id, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(server_id, item_id) DO UPDATE SET
			muxcore_id = COALESCE(NULLIF(excluded.muxcore_id, ''), library_items.muxcore_id),
			title = COALESCE(NULLIF(excluded.title, ''), library_items.title),
			media_type = COALESCE(NULLIF(excluded.media_type, ''), library_items.media_type),
			library_name = COALESCE(NULLIF(excluded.library_name, ''), library_items.library_name),
			media_path = COALESCE(NULLIF(excluded.media_path, ''), library_items.media_path),
			imdb_id = COALESCE(NULLIF(excluded.imdb_id, ''), library_items.imdb_id),
			tmdb_id = CASE WHEN excluded.tmdb_id != 0 THEN excluded.tmdb_id ELSE library_items.tmdb_id END,
			tvdb_id = CASE WHEN excluded.tvdb_id != 0 THEN excluded.tvdb_id ELSE library_items.tvdb_id END,
			updated_at = excluded.updated_at`,
		serverID, itemID, ev.MuxcoreID, ev.Title, ev.MediaType, libraryName, mediaPath, ev.ImdbID, ev.TmdbID, ev.TvdbID, now,
	)
	return err
}

func (m *Module) getLibraryItem(ctx context.Context, serverID, itemID string) (*libraryItemRecord, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	var rec libraryItemRecord
	var updatedAt string
	err := m.queryRow(ctx, `
		SELECT server_id, item_id, muxcore_id, title, media_type, library_name, media_path, file_size_bytes, updated_at
		FROM library_items WHERE server_id = ? AND item_id = ?`, serverID, itemID,
	).Scan(&rec.ServerID, &rec.ItemID, &rec.MuxcoreID, &rec.Title, &rec.MediaType,
		&rec.LibraryName, &rec.MediaPath, &rec.FileSizeBytes, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
	return &rec, nil
}

func (m *Module) countLibraryItems(ctx context.Context, serverID string) (int, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return 0, fmt.Errorf("db not initialized")
	}
	query := `SELECT COUNT(1) FROM library_items`
	args := []any{}
	if strings.TrimSpace(serverID) != "" {
		query += ` WHERE server_id = ?`
		args = append(args, serverID)
	}
	var n int
	err := m.queryRow(ctx, query, args...).Scan(&n)
	return n, err
}
