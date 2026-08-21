package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
)

type libraryCatalogEvent struct {
	Action         string
	ServerID       string
	ServerType     string
	ItemID         string
	MuxcoreID      string
	Title          string
	MediaType      string
	MediaPath      string
	LibraryName    string
	FileSizeBytes  int64
	ImdbID         string
	TmdbID         int64
	TvdbID         int64
	VideoResolution string
	ParentID        string
}

func libraryCatalogFromJSON(sourceModule string, payload []byte) (libraryCatalogEvent, error) {
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return libraryCatalogEvent{}, err
	}
	ev := libraryCatalogEvent{
		Action:     strings.ToLower(strings.TrimSpace(stringField(raw, "action"))),
		ServerID:   stringField(raw, "server_id", "serverId"),
		ServerType: stringField(raw, "server_type", "serverType"),
		ItemID:     stringField(raw, "item_id", "itemId", "ItemId"),
		MuxcoreID:  stringField(raw, "muxcore_id", "muxcoreId"),
		Title:      stringField(raw, "title", "Title", "Name"),
		MediaType:  stringField(raw, "media_type", "mediaType", "itemType"),
		MediaPath:  stringField(raw, "media_path", "mediaPath", "path"),
		LibraryName: stringField(raw, "library_name", "libraryName"),
		FileSizeBytes: int64Field(raw, "file_size_bytes", "fileSizeBytes", "size", "Size"),
	}
	ev.ImdbID, ev.TmdbID, ev.TvdbID = externalIDsFromMap(raw)
	ev.VideoResolution = catalogVideoResolutionFromMap(raw)
	ev.ParentID = strings.TrimSpace(stringField(raw, "parent_id", "parentId", "ParentId", "parent_rating_key", "parentRatingKey"))
	if ev.ServerID == "" {
		ev.ServerID = sourceModule
	}
	if ev.ServerType == "" {
		ev.ServerType = inferServerType(sourceModule)
	}
	if ev.Action == "" {
		return libraryCatalogEvent{}, fmt.Errorf("action required")
	}
	if ev.ItemID == "" {
		return libraryCatalogEvent{}, fmt.Errorf("item_id required")
	}
	return ev, nil
}

func (m *Module) applyLibraryCatalogEvent(ctx context.Context, ev libraryCatalogEvent) error {
	switch ev.Action {
	case "added", "upsert":
		return m.upsertLibraryCatalogItem(ctx, ev)
	case "removed", "delete":
		return m.removeLibraryItem(ctx, ev.ServerID, ev.ItemID)
	default:
		return fmt.Errorf("unknown action %q", ev.Action)
	}
}

func (m *Module) upsertLibraryCatalogItem(ctx context.Context, ev libraryCatalogEvent) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	if err := m.ensureServerRegistered(ctx, db, ev.ServerID, ev.ServerType, ev.ServerType); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := m.exec(ctx, `
		INSERT INTO library_items(
			server_id, item_id, muxcore_id, title, media_type, library_name, media_path, file_size_bytes, catalog_added_at, imdb_id, tmdb_id, tvdb_id, video_resolution, parent_id, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(server_id, item_id) DO UPDATE SET
			muxcore_id = COALESCE(NULLIF(excluded.muxcore_id, ''), library_items.muxcore_id),
			title = COALESCE(NULLIF(excluded.title, ''), library_items.title),
			media_type = COALESCE(NULLIF(excluded.media_type, ''), library_items.media_type),
			library_name = COALESCE(NULLIF(excluded.library_name, ''), library_items.library_name),
			media_path = COALESCE(NULLIF(excluded.media_path, ''), library_items.media_path),
			file_size_bytes = CASE WHEN excluded.file_size_bytes > 0 THEN excluded.file_size_bytes ELSE library_items.file_size_bytes END,
			imdb_id = COALESCE(NULLIF(excluded.imdb_id, ''), library_items.imdb_id),
			tmdb_id = CASE WHEN excluded.tmdb_id != 0 THEN excluded.tmdb_id ELSE library_items.tmdb_id END,
			tvdb_id = CASE WHEN excluded.tvdb_id != 0 THEN excluded.tvdb_id ELSE library_items.tvdb_id END,
			video_resolution = COALESCE(NULLIF(excluded.video_resolution, ''), library_items.video_resolution),
			parent_id = COALESCE(NULLIF(excluded.parent_id, ''), library_items.parent_id),
			removed_at = '',
			catalog_added_at = COALESCE(NULLIF(library_items.catalog_added_at, ''), excluded.catalog_added_at),
			updated_at = excluded.updated_at`,
		ev.ServerID, ev.ItemID, ev.MuxcoreID, ev.Title, ev.MediaType, ev.LibraryName, ev.MediaPath, ev.FileSizeBytes, now, ev.ImdbID, ev.TmdbID, ev.TvdbID, ev.VideoResolution, ev.ParentID, now,
	)
	return err
}

func (m *Module) removeLibraryItem(ctx context.Context, serverID, itemID string) error {
	serverID = strings.TrimSpace(serverID)
	itemID = strings.TrimSpace(itemID)
	if serverID == "" || itemID == "" {
		return nil
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	_, err := m.exec(ctx, `
		UPDATE library_items SET removed_at = ?, updated_at = ?
		WHERE server_id = ? AND item_id = ?`,
		time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339), serverID, itemID,
	)
	return err
}

func (m *Module) handleLibraryItemEvent(evt *eventsv1.Event) {
	if evt == nil || len(evt.Payload) == 0 {
		return
	}
	source := evt.Source
	if source == "" {
		source = "jellyfin"
	}
	catalog, err := libraryCatalogFromJSON(source, evt.Payload)
	if err != nil {
		return
	}
	if err := m.applyLibraryCatalogEvent(context.Background(), catalog); err != nil {
		// catalog sync is best-effort
		_ = err
	}
}

func (m *Module) subscribeLibraryCatalogEvents() {
	mc := m.eventClient()
	if mc == nil {
		return
	}
	ch, cancel, err := mc.Events.Subscribe(context.Background(), "playback.library.item")
	if err != nil {
		return
	}
	go func(events <-chan *eventsv1.Event) {
		defer cancel()
		for evt := range events {
			m.handleLibraryItemEvent(evt)
		}
	}(ch)
}
