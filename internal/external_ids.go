package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func normalizeImdbID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(raw), "tt") {
		return raw
	}
	return "tt" + raw
}

func externalIDsFromMap(raw map[string]any) (imdb string, tmdb, tvdb int64) {
	imdb = strings.TrimSpace(stringField(raw, "imdb_id", "imdbId", "ImdbId", "imdb"))
	if imdb == "" {
		if providers, ok := raw["provider_ids"].(map[string]any); ok {
			imdb = strings.TrimSpace(stringField(providers, "Imdb", "imdb", "imdb_id"))
		}
		if providers, ok := raw["ProviderIds"].(map[string]any); ok {
			if imdb == "" {
				imdb = strings.TrimSpace(stringField(providers, "Imdb", "imdb", "imdb_id"))
			}
			if tmdb == 0 {
				tmdb = int64Field(providers, "Tmdb", "tmdb", "tmdb_id")
			}
			if tvdb == 0 {
				tvdb = int64Field(providers, "Tvdb", "tvdb", "tvdb_id")
			}
		}
	}
	if imdb != "" {
		imdb = normalizeImdbID(imdb)
	}
	if tmdb == 0 {
		tmdb = int64Field(raw, "tmdb_id", "tmdbId", "TmdbId", "tmdb", "themoviedb_id")
	}
	if tvdb == 0 {
		tvdb = int64Field(raw, "tvdb_id", "tvdbId", "TvdbId", "tvdb", "thetvdb_id")
	}
	return imdb, tmdb, tvdb
}

func (m *Module) fillSessionExternalIDs(ctx context.Context, db *sql.DB, serverID string, ev *SessionEvent) {
	if ev == nil || strings.TrimSpace(ev.ItemID) == "" {
		return
	}
	if strings.TrimSpace(ev.ImdbID) != "" && ev.TmdbID != 0 && ev.TvdbID != 0 {
		return
	}
	var imdb string
	var tmdb, tvdb int64
	err := m.queryRow(ctx, `
		SELECT imdb_id, tmdb_id, tvdb_id FROM library_items
		WHERE server_id = ? AND item_id = ? AND `+activeLibraryItemsSQL(""),
		serverID, ev.ItemID,
	).Scan(&imdb, &tmdb, &tvdb)
	if err != nil {
		return
	}
	if strings.TrimSpace(ev.ImdbID) == "" {
		ev.ImdbID = imdb
	}
	if ev.TmdbID == 0 {
		ev.TmdbID = tmdb
	}
	if ev.TvdbID == 0 {
		ev.TvdbID = tvdb
	}
}

const sessionExternalIDCols = `imdb_id, tmdb_id, tvdb_id`

func sessionExternalIDInsertArgs(ev SessionEvent) []any {
	return []any{ev.ImdbID, ev.TmdbID, ev.TvdbID}
}

func externalIDUpdateSQL() string {
	return `imdb_id = COALESCE(NULLIF(?, ''), imdb_id),
		tmdb_id = CASE WHEN ? != 0 THEN ? ELSE tmdb_id END,
		tvdb_id = CASE WHEN ? != 0 THEN ? ELSE tvdb_id END`
}

func externalIDUpdateArgs(ev SessionEvent) []any {
	return []any{ev.ImdbID, ev.TmdbID, ev.TmdbID, ev.TvdbID, ev.TvdbID}
}

func imdbHistoryMatchSQL(alias string) string {
	if alias != "" {
		alias += "."
	}
	return fmt.Sprintf(`(%[1]simdb_id = ? OR (%[1]sitem_id != '' AND %[1]sitem_id IN (
		SELECT item_id FROM library_items li
		WHERE li.server_id = %[1]sserver_id AND li.imdb_id = ? AND `+activeLibraryItemsSQL("li")+`)))`, alias)
}

func tmdbHistoryMatchSQL(alias string) string {
	if alias != "" {
		alias += "."
	}
	return fmt.Sprintf(`(%[1]stmdb_id = ? OR (%[1]sitem_id != '' AND %[1]sitem_id IN (
		SELECT item_id FROM library_items li
		WHERE li.server_id = %[1]sserver_id AND li.tmdb_id = ? AND `+activeLibraryItemsSQL("li")+`)))`, alias)
}

func tvdbHistoryMatchSQL(alias string) string {
	if alias != "" {
		alias += "."
	}
	return fmt.Sprintf(`(%[1]stvdb_id = ? OR (%[1]sitem_id != '' AND %[1]sitem_id IN (
		SELECT item_id FROM library_items li
		WHERE li.server_id = %[1]sserver_id AND li.tvdb_id = ? AND `+activeLibraryItemsSQL("li")+`)))`, alias)
}
