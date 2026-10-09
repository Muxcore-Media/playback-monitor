package internal

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

func (m *Module) handleStopSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rec, err := m.operatorStopSession(r.Context(), r.PathValue("id"))
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "id required") {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session": rec, "stopped": true})
}

func (m *Module) handleListActive(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	rows, err := m.listActiveSessions(r.Context(), r.URL.Query().Get("server_id"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": rows})
}

func (m *Module) handleListHistory(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	rows, total, err := m.listHistory(r.Context(), r.URL.Query().Get("server_id"), r.URL.Query().Get("user_id"), r.URL.Query().Get("q"), limit, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": rows, "total": total})
}

func (m *Module) handleItemStatsHTTP(w http.ResponseWriter, r *http.Request) {
	itemID := strings.TrimSpace(r.URL.Query().Get("item_id"))
	if itemID == "" {
		itemID = strings.TrimSpace(r.URL.Query().Get("id"))
	}
	if itemID == "" {
		http.Error(w, "item_id required", http.StatusBadRequest)
		return
	}
	runtime, _ := strconv.Atoi(r.URL.Query().Get("runtime"))
	ws, err := m.itemWatchStats(r.Context(), itemID, runtime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	lastWatched := ""
	if !ws.LastWatchedAt.IsZero() {
		lastWatched = ws.LastWatchedAt.UTC().Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"item_id":                       itemID,
		"play_count":                    ws.PlayCount,
		"view_count":                    ws.ViewCount,
		"unique_users":                  ws.UniqueUsers,
		"watch_minutes":                 ws.TotalDurationMinutes,
		"longest_minutes":               ws.LongestDurationMinutes,
		"has_activity":                  ws.HasActivity,
		"never_watched":                 ws.NeverWatched,
		"days_since_last_watch":         ws.DaysSinceLastWatch,
		"last_watched_at":               lastWatched,
		"user_watched_percent":          ws.UserWatchedPercent,
		"user_watched_duration_minutes": ws.UserDurationMinutes,
	})
}

func (m *Module) handleHomeStats(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	stats, err := m.homeStats(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stats": stats})
}

func (m *Module) handlePlaysByDateHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	if strings.EqualFold(r.URL.Query().Get("format"), "chart") || r.URL.Query().Get("grouping") != "" || r.URL.Query().Get("user_id") != "" || r.URL.Query().Get("y_axis") != "" {
		userIDs, err := parseChartUserIDs(r.URL.Query().Get("user_id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		grouping := strings.EqualFold(r.URL.Query().Get("grouping"), "1") || strings.EqualFold(r.URL.Query().Get("grouping"), "true")
		chart, err := m.playsByDateChart(r.Context(), playsByDateChartOpts{
			Days:     days,
			UserIDs:  userIDs,
			Grouping: grouping,
			YAxis:    r.URL.Query().Get("y_axis"),
		})
		if err != nil {
			if strings.Contains(err.Error(), "invalid y_axis") {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, playsByDateChartToJSON(chart))
		return
	}
	rows, err := m.playsByDate(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (m *Module) handlePlaysByHourHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	rows, err := m.playsByHour(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (m *Module) handlePlaysByDayOfWeekHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	rows, err := m.playsByDayOfWeek(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (m *Module) handlePlaysByMonthHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	rows, err := m.playsByMonth(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (m *Module) handlePlaysByStreamTypeHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	rows, err := m.playsByStreamType(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (m *Module) handlePlaysByStreamResolutionHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	rows, err := m.playsByStreamResolution(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (m *Module) handlePlaysByTopUsersHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	rows, err := m.playsByTopUsers(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (m *Module) handlePlaysByTopPlatformsHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	rows, err := m.playsByTopPlatforms(r.Context(), days, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (m *Module) handleConcurrentStreamsHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	chart, err := m.concurrentStreamsByStreamType(r.Context(), days)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, chart)
}

func (m *Module) handleLibraryStatsHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	rows, err := m.listLibraryStats(r.Context(), days, r.URL.Query().Get("server_id"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"libraries": rows})
}

func (m *Module) handleLibraryDuplicatesHTTP(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	groups, err := m.listLibraryDuplicates(r.Context(), r.URL.Query().Get("server_id"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"groups": groups})
}

func (m *Module) handleLibraryStaleHTTP(w http.ResponseWriter, r *http.Request) {
	staleDays, _ := strconv.Atoi(r.URL.Query().Get("stale_days"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, never, stale, err := m.listStaleLibraryItems(r.Context(), r.URL.Query().Get("server_id"), staleDays, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":               items,
		"never_watched_count": never,
		"stale_count":         stale,
	})
}

func (m *Module) handleLibraryStorageHTTP(w http.ResponseWriter, r *http.Request) {
	summary, err := m.getLibraryStorageSummary(r.Context(), r.URL.Query().Get("server_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total_items":           summary.TotalItems,
		"total_bytes":           summary.TotalBytes,
		"duplicate_waste_bytes": summary.DuplicateWaste,
		"total_human":           formatBytes(summary.TotalBytes),
		"duplicate_waste_human": formatBytes(summary.DuplicateWaste),
		"libraries":             summary.Libraries,
	})
}

func (m *Module) handleLibraryStorageHistoryHTTP(w http.ResponseWriter, r *http.Request) {
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
	writeJSON(w, http.StatusOK, map[string]any{
		"history": points,
		"prediction": map[string]any{
			"growth_bytes_per_day": pred.GrowthBytesPerDay,
			"projected_bytes":      pred.ProjectedBytes,
			"horizon_days":         pred.HorizonDays,
		},
	})
}

func (m *Module) handleTopContentHTTP(w http.ResponseWriter, r *http.Request) {
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	movies, shows, other, err := m.listTopContent(r.Context(), days, r.URL.Query().Get("server_id"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"movies": movies,
		"shows":  shows,
		"other":  other,
	})
}

func (m *Module) handleIngestHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	var ev SessionEvent
	if unmarshalErr := json.Unmarshal(body, &ev); unmarshalErr != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	id, created, err := m.ingestSessionEvent(r.Context(), ev)
	if errors.Is(err, errSessionKicked) {
		writeJSON(w, http.StatusOK, map[string]any{"session_id": id, "created": false, "stopped": true})
		return
	}
	if errors.Is(err, errUserErased) {
		http.Error(w, errUserErased.Error(), http.StatusForbidden)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session_id": id, "created": created})
}

type importTautulliHTTPRequest struct {
	ServerID    string `json:"server_id"`
	TautulliURL string `json:"tautulli_url"`
	APIKey      string `json:"api_key"`
	RecordsJSON string `json:"records_json"`
	MaxRecords  int32  `json:"max_records"`
	DryRun      bool   `json:"dry_run"`
}

func (m *Module) handleImportTautulliHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	var req importTautulliHTTPRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
	}
	resp, err := m.ImportTautulliHistory(r.Context(), &monitorv1.ImportTautulliHistoryRequest{
		ServerId:    req.ServerID,
		TautulliUrl: req.TautulliURL,
		ApiKey:      req.APIKey,
		RecordsJson: req.RecordsJSON,
		MaxRecords:  req.MaxRecords,
		DryRun:      req.DryRun,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if resp.GetError() != "" && resp.GetImported() == 0 && resp.GetSkipped() == 0 {
		http.Error(w, resp.GetError(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

type importJellystatHTTPRequest struct {
	ServerID   string `json:"server_id"`
	ServerType string `json:"server_type"`
	BackupJSON string `json:"backup_json"`
	MaxRecords int32  `json:"max_records"`
	DryRun     bool   `json:"dry_run"`
}

func (m *Module) handleImportJellystatHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	var req importJellystatHTTPRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
	}
	resp, err := m.ImportJellystatHistory(r.Context(), &monitorv1.ImportJellystatHistoryRequest{
		ServerId:   req.ServerID,
		ServerType: req.ServerType,
		BackupJson: req.BackupJSON,
		MaxRecords: req.MaxRecords,
		DryRun:     req.DryRun,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if resp.GetError() != "" && resp.GetImported() == 0 && resp.GetSkipped() == 0 {
		http.Error(w, resp.GetError(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
