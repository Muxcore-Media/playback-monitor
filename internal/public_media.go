package internal

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type resolvedMedia struct {
	ServerID    string
	ItemID      string
	MuxcoreID   string
	Title       string
	MediaType   string
	LibraryName string
	MediaPath   string
}

func parseMediaRef(ref string) (serverID, itemID, muxcoreID string, err error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", "", "", fmt.Errorf("media ref required")
	}
	if strings.HasPrefix(ref, "muxcore:") {
		return "", "", strings.TrimSpace(strings.TrimPrefix(ref, "muxcore:")), nil
	}
	if idx := strings.Index(ref, ":"); idx > 0 {
		left := strings.TrimSpace(ref[:idx])
		right := strings.TrimSpace(ref[idx+1:])
		if left != "" && right != "" {
			return left, right, "", nil
		}
	}
	return "", ref, "", nil
}

func (m *Module) resolveMediaRef(ctx context.Context, ref string) (*resolvedMedia, error) {
	serverID, itemID, muxcoreID, err := parseMediaRef(ref)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	if muxcoreID != "" {
		var rec resolvedMedia
		err := m.queryRow(ctx, `
			SELECT server_id, item_id, muxcore_id, title, media_type, library_name, media_path
			FROM library_items WHERE muxcore_id = ? LIMIT 1`, muxcoreID,
		).Scan(&rec.ServerID, &rec.ItemID, &rec.MuxcoreID, &rec.Title, &rec.MediaType, &rec.LibraryName, &rec.MediaPath)
		if err == nil {
			return &rec, nil
		}
		if err != sql.ErrNoRows {
			return nil, err
		}
		err = m.queryRow(ctx, `
			SELECT server_id, item_id, muxcore_id,
				MAX(title) AS title, MAX(media_type) AS media_type,
				MAX(library_name) AS library_name, MAX(media_path) AS media_path
			FROM sessions
			WHERE muxcore_id = ?
			GROUP BY server_id, item_id, muxcore_id
			LIMIT 1`, muxcoreID,
		).Scan(&rec.ServerID, &rec.ItemID, &rec.MuxcoreID, &rec.Title, &rec.MediaType, &rec.LibraryName, &rec.MediaPath)
		if err == nil {
			return &rec, nil
		}
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("media not found")
		}
		return nil, err
	}

	if serverID != "" && itemID != "" {
		rec, lookupErr := m.lookupMedia(ctx, db, serverID, itemID)
		if lookupErr == nil {
			return rec, nil
		}
		if lookupErr.Error() != "media not found" {
			return nil, lookupErr
		}
	}

	if itemID != "" {
		var rec resolvedMedia
		err := m.queryRow(ctx, `
			SELECT server_id, item_id, muxcore_id, title, media_type, library_name, media_path
			FROM library_items WHERE item_id = ? ORDER BY updated_at DESC LIMIT 1`, itemID,
		).Scan(&rec.ServerID, &rec.ItemID, &rec.MuxcoreID, &rec.Title, &rec.MediaType, &rec.LibraryName, &rec.MediaPath)
		if err == nil {
			return &rec, nil
		}
		if err != sql.ErrNoRows {
			return nil, err
		}
		err = m.queryRow(ctx, `
			SELECT server_id, item_id, MAX(muxcore_id), MAX(title), MAX(media_type),
				MAX(library_name), MAX(media_path)
			FROM sessions
			WHERE item_id = ?
			GROUP BY server_id, item_id
			ORDER BY MAX(started_at) DESC
			LIMIT 1`, itemID,
		).Scan(&rec.ServerID, &rec.ItemID, &rec.MuxcoreID, &rec.Title, &rec.MediaType, &rec.LibraryName, &rec.MediaPath)
		if err == nil {
			return &rec, nil
		}
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("media not found")
		}
		return nil, err
	}
	return nil, fmt.Errorf("media not found")
}

func (m *Module) lookupMedia(ctx context.Context, db *sql.DB, serverID, itemID string) (*resolvedMedia, error) {
	var rec resolvedMedia
	err := m.queryRow(ctx, `
		SELECT server_id, item_id, muxcore_id, title, media_type, library_name, media_path
		FROM library_items WHERE server_id = ? AND item_id = ?`, serverID, itemID,
	).Scan(&rec.ServerID, &rec.ItemID, &rec.MuxcoreID, &rec.Title, &rec.MediaType, &rec.LibraryName, &rec.MediaPath)
	if err == nil {
		return &rec, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}
	err = m.queryRow(ctx, `
		SELECT server_id, item_id, MAX(muxcore_id), MAX(title), MAX(media_type),
			MAX(library_name), MAX(media_path)
		FROM sessions
		WHERE server_id = ? AND item_id = ?
		GROUP BY server_id, item_id`, serverID, itemID,
	).Scan(&rec.ServerID, &rec.ItemID, &rec.MuxcoreID, &rec.Title, &rec.MediaType, &rec.LibraryName, &rec.MediaPath)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("media not found")
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func mediaRefString(rec *resolvedMedia) string {
	if rec == nil {
		return ""
	}
	if strings.TrimSpace(rec.MuxcoreID) != "" {
		return "muxcore:" + rec.MuxcoreID
	}
	return rec.ServerID + ":" + rec.ItemID
}

func mediaMatchWhere(alias string) string {
	if alias != "" {
		alias += "."
	}
	return fmt.Sprintf(`(%[1]sitem_id = ? OR (? != '' AND %[1]smuxcore_id = ?))`, alias)
}

func (m *Module) mediaWindowStats(ctx context.Context, rec *resolvedMedia) (map[string]userWindowStats, error) {
	windows := map[string]int{"last_7": 7, "last_30": 30}
	out := map[string]userWindowStats{"all_time": {}, "last_7": {}, "last_30": {}}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	match := mediaMatchWhere("")
	for key, days := range windows {
		since := time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339)
		var plays int
		var minutes float64
		err := m.queryRow(ctx, `
			SELECT COUNT(1), COALESCE(SUM(position_seconds), 0) / 60.0
			FROM sessions
			WHERE state = 'stopped' AND started_at >= ? AND `+match,
			since, rec.ItemID, rec.MuxcoreID, rec.MuxcoreID,
		).Scan(&plays, &minutes)
		if err != nil {
			return nil, err
		}
		out[key] = userWindowStats{Plays: plays, WatchMinutes: minutes}
	}
	var plays int
	var minutes float64
	err := m.queryRow(ctx, `
		SELECT COUNT(1), COALESCE(SUM(position_seconds), 0) / 60.0
		FROM sessions
		WHERE state = 'stopped' AND `+match, rec.ItemID, rec.MuxcoreID, rec.MuxcoreID,
	).Scan(&plays, &minutes)
	if err != nil {
		return nil, err
	}
	out["all_time"] = userWindowStats{Plays: plays, WatchMinutes: minutes}
	return out, nil
}

type mediaWatcherRow struct {
	UserID       string
	UserName     string
	PlayCount    int
	WatchMinutes float64
}

func (m *Module) listMediaWatchers(ctx context.Context, rec *resolvedMedia, limit int) ([]mediaWatcherRow, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	match := mediaMatchWhere("")
	rows, err := m.queryRows(ctx, `
		SELECT user_id, user_name, COUNT(1), COALESCE(SUM(position_seconds), 0) / 60.0
		FROM sessions
		WHERE state = 'stopped' AND `+match+`
		GROUP BY user_id, user_name
		ORDER BY COUNT(1) DESC
		LIMIT ?`, rec.ItemID, rec.MuxcoreID, rec.MuxcoreID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]mediaWatcherRow, 0)
	for rows.Next() {
		var row mediaWatcherRow
		if err := rows.Scan(&row.UserID, &row.UserName, &row.PlayCount, &row.WatchMinutes); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (m *Module) listMediaHistoryPage(ctx context.Context, rec *resolvedMedia, pageSize int, cursorRaw string) ([]SessionRecord, string, error) {
	if pageSize <= 0 {
		pageSize = 25
	}
	if pageSize > 100 {
		pageSize = 100
	}
	cursorStarted, cursorID, err := decodeHistoryCursor(cursorRaw)
	if err != nil {
		return nil, "", err
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, "", fmt.Errorf("db not initialized")
	}
	where := []string{`state = 'stopped'`, mediaMatchWhere("")}
	args := []any{rec.ItemID, rec.MuxcoreID, rec.MuxcoreID}
	if !cursorStarted.IsZero() {
		where = append(where, `(started_at < ? OR (started_at = ? AND id < ?))`)
		ts := cursorStarted.Format(time.RFC3339Nano)
		args = append(args, ts, ts, cursorID)
	}
	query := `SELECT ` + sessionSelectCols + `
		FROM sessions WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY started_at DESC, id DESC
		LIMIT ?`
	listArgs := append(append([]any{}, args...), pageSize+1)
	rows, err := m.querySessions(ctx, db, query, listArgs...)
	if err != nil {
		return nil, "", err
	}
	var nextCursor string
	if len(rows) > pageSize {
		last := rows[pageSize-1]
		nextCursor = encodeHistoryCursor(last.StartedAt, last.ID)
		rows = rows[:pageSize]
	}
	return rows, nextCursor, nil
}

func publicMediaJSON(rec *resolvedMedia, seasonCount, episodeCount int) map[string]any {
	availability := []map[string]any{{
		"server_id":     rec.ServerID,
		"item_id":       rec.ItemID,
		"library_name":  rec.LibraryName,
		"media_path":    rec.MediaPath,
		"status":        "available",
	}}
	out := map[string]any{
		"ref":          mediaRefString(rec),
		"title":        rec.Title,
		"media_type":   rec.MediaType,
		"muxcore_id":   rec.MuxcoreID,
		"library_name": rec.LibraryName,
		"availability": availability,
	}
	if seasonCount > 0 {
		out["season_count"] = seasonCount
	}
	if episodeCount > 0 {
		out["episode_count"] = episodeCount
	}
	return out
}

func (m *Module) handlePublicMedia(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	rec, err := m.resolveMediaRef(r.Context(), r.PathValue("ref"))
	if err != nil {
		http.Error(w, "media not found", http.StatusNotFound)
		return
	}
	seasons, episodes, err := m.countMediaDescendants(r.Context(), rec)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, publicMediaJSON(rec, seasons, episodes))
}

func (m *Module) handlePublicMediaStats(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	rec, err := m.resolveMediaRef(r.Context(), r.PathValue("ref"))
	if err != nil {
		http.Error(w, "media not found", http.StatusNotFound)
		return
	}
	stats, err := m.mediaWindowStats(r.Context(), rec)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	windows := make(map[string]any, len(stats))
	for key, stat := range stats {
		windows[key] = map[string]any{
			"plays":         stat.Plays,
			"watch_minutes": stat.WatchMinutes,
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ref":     mediaRefString(rec),
		"title":   rec.Title,
		"windows": windows,
	})
}

func (m *Module) handlePublicMediaWatchers(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	rec, err := m.resolveMediaRef(r.Context(), r.PathValue("ref"))
	if err != nil {
		http.Error(w, "media not found", http.StatusNotFound)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	rows, err := m.listMediaWatchers(r.Context(), rec, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		user := row.UserName
		if user == "" {
			user = row.UserID
		}
		data = append(data, map[string]any{
			"user_id":       row.UserID,
			"username":      user,
			"play_count":    row.PlayCount,
			"watch_minutes": row.WatchMinutes,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}

func (m *Module) handlePublicMediaHistory(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	rec, err := m.resolveMediaRef(r.Context(), r.PathValue("ref"))
	if err != nil {
		http.Error(w, "media not found", http.StatusNotFound)
		return
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize == 0 {
		pageSize, _ = strconv.Atoi(r.URL.Query().Get("page_size"))
	}
	rows, nextCursor, err := m.listMediaHistoryPage(r.Context(), rec, pageSize, r.URL.Query().Get("cursor"))
	if err != nil {
		if strings.Contains(err.Error(), "invalid cursor") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if pageSize <= 0 {
		pageSize = 25
	}
	data := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		data = append(data, publicHistoryFromRecord(row))
	}
	meta := map[string]any{"nextCursor": nil, "pageSize": pageSize}
	if nextCursor != "" {
		meta["nextCursor"] = nextCursor
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": meta})
}

func (m *Module) handlePublicMediaChildren(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	rec, err := m.resolveMediaRef(r.Context(), r.PathValue("ref"))
	if err != nil {
		http.Error(w, "media not found", http.StatusNotFound)
		return
	}
	if !mediaHasChildren(rec.MediaType) {
		http.Error(w, "media has no children", http.StatusNotFound)
		return
	}
	rows, err := m.listMediaChildren(r.Context(), rec)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		data = append(data, publicMediaChildRow(row))
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}
