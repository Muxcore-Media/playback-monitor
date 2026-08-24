package internal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (m *Module) publicAPIEnabled() bool {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return strings.TrimSpace(m.publicAPIKey) != ""
}

func (m *Module) requirePublicAPIAuth(w http.ResponseWriter, r *http.Request) bool {
	if !m.publicAPIEnabled() {
		http.Error(w, "public API disabled; set PLAYBACK_MONITOR_PUBLIC_API_KEY", http.StatusServiceUnavailable)
		return false
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, "missing or invalid Authorization header", http.StatusUnauthorized)
		return false
	}
	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	m.cfgMu.RLock()
	expected := m.publicAPIKey
	m.cfgMu.RUnlock()
	if token == "" || token != expected {
		http.Error(w, "invalid API key", http.StatusUnauthorized)
		return false
	}
	return true
}

type historyCursor struct {
	StartedAt string `json:"started_at"`
	ID        string `json:"id"`
}

func encodeHistoryCursor(startedAt time.Time, id string) string {
	payload, _ := json.Marshal(historyCursor{
		StartedAt: startedAt.UTC().Format(time.RFC3339Nano),
		ID:        id,
	})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeHistoryCursor(raw string) (time.Time, string, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, "", nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	var cur historyCursor
	if unmarshalErr := json.Unmarshal(data, &cur); unmarshalErr != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	if strings.TrimSpace(cur.ID) == "" || strings.TrimSpace(cur.StartedAt) == "" {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	started, err := time.Parse(time.RFC3339Nano, cur.StartedAt)
	if err != nil {
		started, err = time.Parse(time.RFC3339, cur.StartedAt)
	}
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	return started.UTC(), cur.ID, nil
}

func (m *Module) listHistoryPageByIdentity(ctx context.Context, serverID, identityID string, pageSize int, cursorRaw string) ([]SessionRecord, string, error) {
	return m.listHistoryPageFiltered(ctx, historyPageFilter{
		ServerID:   serverID,
		IdentityID: identityID,
	}, pageSize, cursorRaw)
}

func publicStreamFromRecord(rec SessionRecord) map[string]any {
	return map[string]any{
		"id":               rec.ID,
		"server_id":        rec.ServerID,
		"server_type":      rec.ServerType,
		"username":         rec.UserName,
		"user_id":          rec.UserID,
		"media_title":      rec.Title,
		"media_type":       rec.MediaType,
		"item_id":          rec.ItemID,
		"muxcore_id":       rec.MuxcoreID,
		"state":            rec.State,
		"progress_seconds": rec.PositionSeconds,
		"duration_seconds": rec.DurationSeconds,
		"started_at":       rec.StartedAt.UTC().Format(time.RFC3339),
		"is_transcode":     rec.IsTranscode,
		"play_method":      rec.PlayMethod,
		"platform":         rec.Platform,
		"device":           rec.Device,
		"player":           rec.Player,
		"ip_address":       rec.IPAddress,
		"geo_country":      rec.GeoCountry,
		"geo_city":         rec.GeoCity,
	}
}

func publicHistoryFromRecord(rec SessionRecord) map[string]any {
	out := publicStreamFromRecord(rec)
	out["stopped_at"] = rec.StoppedAt.UTC().Format(time.RFC3339)
	if rec.ImdbID != "" {
		out["imdb_id"] = rec.ImdbID
	}
	if rec.TmdbID != 0 {
		out["tmdb_id"] = rec.TmdbID
	}
	if rec.TvdbID != 0 {
		out["tvdb_id"] = rec.TvdbID
	}
	return out
}

func (m *Module) handlePublicStreams(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	serverID := r.URL.Query().Get("server_id")
	summaryOnly := strings.EqualFold(r.URL.Query().Get("summary"), "true")
	rows, err := m.listActiveSessions(r.Context(), serverID, 500)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := make([]map[string]any, 0)
	if !summaryOnly {
		for _, rec := range rows {
			data = append(data, publicStreamFromRecord(rec))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data":    data,
		"summary": m.buildStreamSummary(r.Context(), rows),
	})
}

func (m *Module) handlePublicHistory(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize == 0 {
		pageSize, _ = strconv.Atoi(r.URL.Query().Get("page_size"))
	}
	filter, err := parseHistoryPageFilter(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rows, nextCursor, err := m.listHistoryPageFiltered(
		r.Context(),
		filter,
		pageSize,
		r.URL.Query().Get("cursor"),
	)
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
	for _, rec := range rows {
		data = append(data, publicHistoryFromRecord(rec))
	}
	meta := map[string]any{
		"nextCursor": nil,
		"pageSize":   pageSize,
	}
	if nextCursor != "" {
		meta["nextCursor"] = nextCursor
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": meta})
}

func (m *Module) handlePublicHomeStats(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	stats, err := m.homeStats(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]map[string]any, 0, len(stats))
	for _, stat := range stats {
		out = append(out, map[string]any{
			"key":   stat.Key,
			"label": stat.Label,
			"value": stat.Value,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

func (m *Module) handlePublicConcurrentStats(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	rows, err := m.concurrentStreamRows(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		data = append(data, map[string]any{
			"date":      row.Date,
			"total":     row.Total,
			"direct":    row.Direct,
			"transcode": row.Transcode,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}

func (m *Module) handlePublicLibraries(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	serverID := r.URL.Query().Get("server_id")
	rows, err := m.listLibraryCatalogRollups(r.Context(), serverID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	playsByKey := map[string]libraryStatRow{}
	if playsDays, _ := strconv.Atoi(r.URL.Query().Get("plays_days")); playsDays > 0 {
		playRows, err := m.listLibraryStats(r.Context(), playsDays, serverID, 500)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, row := range playRows {
			playsByKey[row.ServerID+"\x00"+row.LibraryName] = row
		}
	}

	data := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		entry := map[string]any{
			"server_id":       row.ServerID,
			"server_type":     row.ServerType,
			"library_id":      row.LibraryName,
			"library_name":    row.LibraryName,
			"item_count":      row.ItemCount,
			"movie_count":     row.MovieCount,
			"episode_count":   row.EpisodeCount,
			"show_count":      row.ShowCount,
			"track_count":     row.TrackCount,
			"total_file_size": row.TotalFileSize,
			"resolutions":     row.Resolutions,
		}
		if play, ok := playsByKey[row.ServerID+"\x00"+row.LibraryName]; ok {
			entry["play_count"] = play.PlayCount
			entry["watch_minutes"] = play.WatchMinutes
		}
		data = append(data, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}

func (m *Module) handlePublicLibraryDuplicates(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	groups, err := m.listLibraryDuplicates(r.Context(), r.URL.Query().Get("server_id"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		copies := make([]map[string]any, 0, len(g.Copies))
		for _, c := range g.Copies {
			copies = append(copies, map[string]any{
				"server_id":       c.ServerID,
				"item_id":         c.ItemID,
				"title":           c.Title,
				"library_name":    c.LibraryName,
				"media_path":      c.MediaPath,
				"file_size_bytes": c.FileSizeBytes,
			})
		}
		data = append(data, map[string]any{
			"group_key":  g.GroupKey,
			"title":      g.Title,
			"copy_count": g.CopyCount,
			"copies":     copies,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}

func (m *Module) handlePublicLibraryStale(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	staleDays, _ := strconv.Atoi(r.URL.Query().Get("stale_days"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, never, stale, err := m.listStaleLibraryItems(r.Context(), r.URL.Query().Get("server_id"), staleDays, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := make([]map[string]any, 0, len(items))
	for _, item := range items {
		lastWatched := ""
		if item.LastWatched != nil {
			lastWatched = item.LastWatched.UTC().Format(time.RFC3339)
		}
		data = append(data, map[string]any{
			"server_id":       item.ServerID,
			"item_id":         item.ItemID,
			"title":           item.Title,
			"media_type":      item.MediaType,
			"library_name":    item.LibraryName,
			"file_size_bytes": item.FileSizeBytes,
			"last_watched":    lastWatched,
			"watch_count":     item.WatchCount,
			"category":        item.Category,
			"days_stale":      item.DaysStale,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data": data,
		"meta": map[string]any{
			"never_watched_count": never,
			"stale_count":         stale,
		},
	})
}

func (m *Module) handlePublicLibraryStorage(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	summary, err := m.getLibraryStorageSummary(r.Context(), r.URL.Query().Get("server_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	libraries := make([]map[string]any, 0, len(summary.Libraries))
	for _, row := range summary.Libraries {
		libraries = append(libraries, map[string]any{
			"server_id":    row.ServerID,
			"library_name": row.LibraryName,
			"item_count":   row.ItemCount,
			"total_bytes":  row.TotalBytes,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"total_items":           summary.TotalItems,
			"total_bytes":           summary.TotalBytes,
			"duplicate_waste_bytes": summary.DuplicateWaste,
			"libraries":             libraries,
		},
	})
}

func (m *Module) handlePublicLibraryStorageHistory(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	predictDays, _ := strconv.Atoi(r.URL.Query().Get("predict_days"))
	points, err := m.getLibraryStorageHistory(r.Context(), r.URL.Query().Get("server_id"), firstNonEmptyStr(r.URL.Query().Get("library_id"), r.URL.Query().Get("library_name")), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if predictDays <= 0 {
		predictDays = 90
	}
	pred := predictLibraryStorageGrowth(points, predictDays)
	history := make([]map[string]any, 0, len(points))
	for _, p := range points {
		history = append(history, map[string]any{
			"day":         p.Day,
			"total_bytes": p.TotalBytes,
			"item_count":  p.ItemCount,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"history": history,
			"prediction": map[string]any{
				"growth_bytes_per_day": pred.GrowthBytesPerDay,
				"projected_bytes":      pred.ProjectedBytes,
				"horizon_days":         pred.HorizonDays,
			},
		},
	})
}

func (m *Module) handlePublicTopContent(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	movies, shows, other, err := m.listTopContent(r.Context(), days, r.URL.Query().Get("server_id"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"movies": topContentPublicRows(movies),
			"shows":  topContentPublicRows(shows),
			"other":  topContentPublicRows(other),
		},
	})
}

func topContentPublicRows(rows []topContentRow) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"title":         row.Title,
			"media_type":    row.MediaType,
			"play_count":    row.PlayCount,
			"watch_minutes": row.WatchMinutes,
		})
	}
	return out
}

func (m *Module) handlePublicRecentlyAdded(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize == 0 {
		pageSize, _ = strconv.Atoi(r.URL.Query().Get("page_size"))
	}
	includeRemoved := strings.EqualFold(r.URL.Query().Get("include_removed"), "true") ||
		r.URL.Query().Get("include_removed") == "1"
	items, nextCursor, err := m.listRecentlyAddedLibraryItems(
		r.Context(),
		r.URL.Query().Get("server_id"),
		r.URL.Query().Get("library_id"),
		r.URL.Query().Get("media_type"),
		includeRemoved,
		pageSize,
		r.URL.Query().Get("cursor"),
	)
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
	data := make([]map[string]any, 0, len(items))
	for _, item := range items {
		data = append(data, recentlyAddedItemJSON(item))
	}
	meta := map[string]any{"nextCursor": nil, "pageSize": pageSize}
	if nextCursor != "" {
		meta["nextCursor"] = nextCursor
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": meta})
}

func (m *Module) handlePublicDocs(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":       "MuxCore Playback Monitor Public API v2",
			"version":     "0.1.0",
			"description": "Read-only subset aligned with Tracearr /api/v2/public routes.",
		},
		"components": map[string]any{
			"securitySchemes": map[string]any{
				"bearerAuth": map[string]any{
					"type":        "http",
					"scheme":      "bearer",
					"description": "Bearer token from PLAYBACK_MONITOR_PUBLIC_API_KEY",
				},
			},
		},
		"security": []map[string]any{{"bearerAuth": []string{}}},
		"paths": map[string]any{
			"/api/v2/public/health": map[string]any{
				"get": map[string]any{"summary": "Monitor health and registered server connectivity (active streams + last activity)"},
			},
			"/api/v2/public/servers": map[string]any{
				"get": map[string]any{"summary": "Registered media servers with active session counts"},
			},
			"/api/v2/public/streams": map[string]any{
				"get": map[string]any{"summary": "Active playback sessions"},
			},
			"/api/v2/public/history": map[string]any{
				"get": map[string]any{"summary": "Stopped session history with cursor pagination; filters: user_id, media_id, rating_key, media_type, imdb_id, tmdb_id, tvdb_id, since, until, watched"},
			},
			"/api/v2/public/stats/home": map[string]any{
				"get": map[string]any{"summary": "Home dashboard stats"},
			},
			"/api/v2/public/stats/concurrent": map[string]any{
				"get": map[string]any{"summary": "Peak concurrent streams per day (total, direct, transcode)"},
			},
			"/api/v2/public/stats/plays/platforms": map[string]any{
				"get": map[string]any{"summary": "Top client platforms by play count"},
			},
			"/api/v2/public/stats/plays/stream-type": map[string]any{
				"get": map[string]any{"summary": "Direct play vs transcode play counts"},
			},
			"/api/v2/public/stats/plays/stream-resolution": map[string]any{
				"get": map[string]any{"summary": "Play counts grouped by delivered stream resolution"},
			},
			"/api/v2/public/stats/plays/source-resolution": map[string]any{
				"get": map[string]any{"summary": "Play counts grouped by catalog file resolution (Tautulli source resolution)"},
			},
			"/api/v2/public/stats/plays/platform-resolution": map[string]any{
				"get": map[string]any{"summary": "Top platform and stream-resolution combinations"},
			},
			"/api/v2/public/stats/plays/by-date": map[string]any{
				"get": map[string]any{"summary": "Tautulli-style plays-by-date chart (categories + TV/Movies/Music/Live TV series); query: days, user_id, grouping, y_axis=plays|duration"},
			},
			"/api/v2/public/stats/plays/by-hour": map[string]any{
				"get": map[string]any{"summary": "Play counts grouped by hour of day"},
			},
			"/api/v2/public/stats/plays/by-dow": map[string]any{
				"get": map[string]any{"summary": "Play counts grouped by day of week"},
			},
			"/api/v2/public/stats/plays/by-month": map[string]any{
				"get": map[string]any{"summary": "Play counts grouped by calendar month"},
			},
			"/api/v2/public/stats/plays/top-users": map[string]any{
				"get": map[string]any{"summary": "Top users by play count"},
			},
			"/api/v2/public/libraries": map[string]any{
				"get": map[string]any{"summary": "Per-library catalog rollups (item counts, file size, resolution mix); optional plays_days merges session play stats"},
			},
			"/api/v2/public/recently-added": map[string]any{
				"get": map[string]any{"summary": "Library catalog items newest first by catalog added date"},
			},
			"/api/v2/public/library/duplicates": map[string]any{
				"get": map[string]any{"summary": "Cross-server duplicate titles grouped by muxcore id or normalized title"},
			},
			"/api/v2/public/library/stale": map[string]any{
				"get": map[string]any{"summary": "Never-watched and stale catalog items (default 90-day threshold)"},
			},
			"/api/v2/public/library/storage": map[string]any{
				"get": map[string]any{"summary": "Catalog storage totals, per-library breakdown, and duplicate waste estimate"},
			},
			"/api/v2/public/library/storage/history": map[string]any{
				"get": map[string]any{"summary": "Daily storage snapshots with linear growth projection; optional library_id/library_name for per-library trend"},
			},
			"/api/v2/public/stats/top-content": map[string]any{
				"get": map[string]any{"summary": "Top movies and shows by play count"},
			},
			"/api/v2/public/users": map[string]any{
				"get": map[string]any{"summary": "Watchers with per-server account correlation"},
			},
			"/api/v2/public/users/{id}": map[string]any{
				"get": map[string]any{"summary": "One watcher identity"},
			},
			"/api/v2/public/users/{id}/stats": map[string]any{
				"get": map[string]any{"summary": "Play counts and watch time by window"},
			},
			"/api/v2/public/users/{id}/history": map[string]any{
				"get": map[string]any{"summary": "Cursor-paginated watch history for a watcher"},
			},
			"/api/v2/public/media/{ref}": map[string]any{
				"get": map[string]any{"summary": "Resolve media ref with availability and season/episode counts for shows"},
			},
			"/api/v2/public/media/{ref}/children": map[string]any{
				"get": map[string]any{"summary": "Seasons of a show or episodes of a season via catalog parent_id"},
			},
			"/api/v2/public/media/{ref}/stats": map[string]any{
				"get": map[string]any{"summary": "Play counts and watch time by window for media"},
			},
			"/api/v2/public/media/{ref}/watchers": map[string]any{
				"get": map[string]any{"summary": "Users who watched this media"},
			},
			"/api/v2/public/media/{ref}/history": map[string]any{
				"get": map[string]any{"summary": "Cursor-paginated watch history for media"},
			},
		},
	})
}
