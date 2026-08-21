package internal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

type tautulliImportStats struct {
	Imported      int
	Skipped       int
	Failed        int
	TotalFetched  int
}

func (m *Module) ImportTautulliHistory(ctx context.Context, req *monitorv1.ImportTautulliHistoryRequest) (*monitorv1.ImportTautulliHistoryResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request required")
	}
	serverID := strings.TrimSpace(req.GetServerId())
	if serverID == "" {
		serverID = "tautulli"
	}
	maxRecords := int(req.GetMaxRecords())
	if maxRecords <= 0 {
		maxRecords = 50000
	}

	var records []map[string]any
	var err error
	if raw := strings.TrimSpace(req.GetRecordsJson()); raw != "" {
		records, err = parseTautulliRecordsJSON(raw)
	} else if base := strings.TrimSpace(req.GetTautulliUrl()); base != "" {
		key := strings.TrimSpace(req.GetApiKey())
		if key == "" {
			return nil, fmt.Errorf("api_key required when tautulli_url is set")
		}
		records, err = fetchAllTautulliHistory(ctx, base, key, maxRecords)
	} else {
		return nil, fmt.Errorf("records_json or tautulli_url+api_key required")
	}
	if err != nil {
		return &monitorv1.ImportTautulliHistoryResponse{Error: err.Error()}, nil
	}
	if len(records) > maxRecords {
		records = records[:maxRecords]
	}

	stats, importErr := m.importTautulliRecords(ctx, serverID, records, req.GetDryRun())
	resp := &monitorv1.ImportTautulliHistoryResponse{
		Imported:     int32(stats.Imported),
		Skipped:      int32(stats.Skipped),
		Failed:       int32(stats.Failed),
		TotalFetched: int32(stats.TotalFetched),
	}
	if importErr != nil {
		resp.Error = importErr.Error()
	}
	return resp, nil
}

func (m *Module) importTautulliRecords(ctx context.Context, serverID string, records []map[string]any, dryRun bool) (tautulliImportStats, error) {
	stats := tautulliImportStats{TotalFetched: len(records)}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return stats, fmt.Errorf("db not initialized")
	}
	if err := m.ensureServerRegistered(ctx, db, serverID, "plex", "tautulli-import"); err != nil {
		return stats, err
	}

	batchSeen := map[string]struct{}{}
	for _, rec := range records {
		externalID, ev, startedAt, stoppedAt, ok := tautulliRecordToSessionEvent(rec, serverID)
		if !ok {
			stats.Failed++
			continue
		}
		if _, dup := batchSeen[externalID]; dup {
			stats.Skipped++
			continue
		}
		exists, err := m.importSessionExists(ctx, serverID, externalID)
		if err != nil {
			return stats, err
		}
		if exists {
			stats.Skipped++
			continue
		}
		batchSeen[externalID] = struct{}{}
		if dryRun {
			stats.Imported++
			continue
		}
		if err := m.insertImportedSession(ctx, externalID, ev, startedAt, stoppedAt); err != nil {
			stats.Failed++
			continue
		}
		stats.Imported++
	}
	return stats, nil
}

func tautulliRecordToSessionEvent(rec map[string]any, serverID string) (externalID string, ev SessionEvent, startedAt, stoppedAt time.Time, ok bool) {
	if rec == nil {
		return "", SessionEvent{}, time.Time{}, time.Time{}, false
	}
	ref := coerceInt64(rec["reference_id"])
	row := coerceInt64(rec["row_id"])
	switch {
	case ref > 0:
		externalID = fmt.Sprintf("tautulli:ref:%d", ref)
	case row > 0:
		externalID = fmt.Sprintf("tautulli:row:%d", row)
	default:
		sum := sha256.Sum256([]byte(fmt.Sprintf("%v", rec)))
		externalID = "tautulli:hash:" + hex.EncodeToString(sum[:8])
	}

	started := coerceInt64(rec["started"])
	stopped := coerceInt64(rec["stopped"])
	if started <= 0 {
		started = coerceInt64(rec["date"])
	}
	if stopped <= 0 && started > 0 {
		stopped = started + coerceInt64(rec["play_duration"])
	}
	if started <= 0 {
		return "", SessionEvent{}, time.Time{}, time.Time{}, false
	}

	title := firstNonEmptyStr(
		coerceString(rec["full_title"]),
		coerceString(rec["title"]),
		coerceString(rec["grandparent_title"]),
	)
	userName := firstNonEmptyStr(coerceString(rec["friendly_name"]), coerceString(rec["user"]))
	userID := coerceString(rec["user_id"])
	if userID == "" && coerceInt64(rec["user_id"]) > 0 {
		userID = strconv.FormatInt(coerceInt64(rec["user_id"]), 10)
	}

	transcode := strings.EqualFold(coerceString(rec["transcode_decision"]), "transcode")
	playDuration := coerceInt64(rec["play_duration"])
	if playDuration <= 0 {
		playDuration = coerceInt64(rec["duration"])
	}
	totalDuration := coerceInt64(rec["duration"])
	if totalDuration <= 0 {
		totalDuration = playDuration
	}

	startedAt = time.Unix(started, 0).UTC()
	stoppedAt = time.Unix(stopped, 0).UTC()
	if stopped <= 0 {
		stoppedAt = startedAt
	}

	ev = SessionEvent{
		EventType:         "playback.stopped",
		SourceModule:      "tautulli-import",
		ServerID:          serverID,
		ServerType:        "plex",
		ExternalSessionID: externalID,
		UserID:            userID,
		UserName:          userName,
		ItemID:            coerceString(rec["rating_key"]),
		Title:             title,
		MediaType:         coerceString(rec["media_type"]),
		PositionSeconds:   playDuration,
		DurationSeconds:   totalDuration,
		IsTranscode:       transcode,
		Platform:          coerceString(rec["platform"]),
		Device:            coerceString(rec["machine_id"]),
		Player:            firstNonEmptyStr(coerceString(rec["player"]), coerceString(rec["product"])),
		IPAddress:         coerceString(rec["ip_address"]),
		GeoCountry:        coerceString(rec["location"]),
		StreamResolution: normalizeStreamResolution(
			int(coerceInt64(rec["transcode_height"])),
			0,
			firstNonEmptyStr(
				coerceString(rec["stream_video_full_resolution"]),
				coerceString(rec["video_full_resolution"]),
				coerceString(rec["video_resolution"]),
			),
		),
		OccurredAt: stoppedAt,
	}
	return externalID, ev, startedAt, stoppedAt, true
}

func (m *Module) insertImportedSession(ctx context.Context, externalID string, ev SessionEvent, startedAt, stoppedAt time.Time) error {
	if startedAt.IsZero() {
		startedAt = stoppedAt
	}
	if stoppedAt.IsZero() {
		stoppedAt = startedAt
	}
	startStr := startedAt.UTC().Format(time.RFC3339)
	stopStr := stoppedAt.UTC().Format(time.RFC3339)
	geoCountry, geoCity, geoLat, geoLon := geoInsertValues(ev)

	id := uuid.NewString()
	_, err := m.exec(ctx, `
		INSERT INTO sessions(
			id, server_id, server_type, external_session_id, state, user_id, user_name, item_id, muxcore_id,
			title, media_type, started_at, stopped_at, last_progress_at, position_seconds, duration_seconds,
			is_transcode, platform, device, player, ip_address, source_module, geo_country, geo_city, geo_lat, geo_lon,
			stream_resolution, imdb_id, tmdb_id, tvdb_id
		) VALUES (?, ?, ?, ?, 'stopped', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, ev.ServerID, ev.ServerType, externalID, ev.UserID, ev.UserName, ev.ItemID, ev.MuxcoreID,
		ev.Title, ev.MediaType, startStr, stopStr, stopStr,
		float64(ev.PositionSeconds), float64(ev.DurationSeconds), boolToInt(ev.IsTranscode),
		ev.Platform, ev.Device, ev.Player, ev.IPAddress, ev.SourceModule,
		geoCountry, geoCity, geoLat, geoLon, ev.StreamResolution, ev.ImdbID, ev.TmdbID, ev.TvdbID,
	)
	return err
}

func (m *Module) importSessionExists(ctx context.Context, serverID, externalID string) (bool, error) {
	var n int
	err := m.queryRow(ctx,
		`SELECT COUNT(1) FROM sessions WHERE server_id = ? AND external_session_id = ?`,
		serverID, externalID,
	).Scan(&n)
	return n > 0, err
}

func parseTautulliRecordsJSON(raw string) ([]map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty records_json")
	}
	var direct []map[string]any
	if err := json.Unmarshal([]byte(raw), &direct); err == nil && len(direct) > 0 {
		return direct, nil
	}
	var wrapper struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err == nil && len(wrapper.Records) > 0 {
		return wrapper.Records, nil
	}
	var api struct {
		Response struct {
			Data struct {
				Data []map[string]any `json:"data"`
			} `json:"data"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(raw), &api); err != nil {
		return nil, fmt.Errorf("parse records_json: %w", err)
	}
	if len(api.Response.Data.Data) == 0 {
		return nil, fmt.Errorf("no records found in json payload")
	}
	return api.Response.Data.Data, nil
}

func fetchAllTautulliHistory(ctx context.Context, baseURL, apiKey string, maxRecords int) ([]map[string]any, error) {
	const pageSize = 1000
	out := make([]map[string]any, 0)
	start := 0
	for len(out) < maxRecords {
		page, total, err := fetchTautulliHistoryPage(ctx, baseURL, apiKey, start, pageSize)
		if err != nil {
			return out, err
		}
		if len(page) == 0 {
			break
		}
		out = append(out, page...)
		start += len(page)
		if start >= total || len(page) < pageSize {
			break
		}
	}
	if len(out) > maxRecords {
		out = out[:maxRecords]
	}
	return out, nil
}

func fetchTautulliHistoryPage(ctx context.Context, baseURL, apiKey string, start, length int) ([]map[string]any, int, error) {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	q := url.Values{}
	q.Set("apikey", apiKey)
	q.Set("cmd", "get_history")
	q.Set("start", strconv.Itoa(start))
	q.Set("length", strconv.Itoa(length))
	q.Set("order_column", "date")
	q.Set("order_dir", "desc")
	reqURL := base + "/api/v2?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("tautulli api http %d", resp.StatusCode)
	}
	records, err := parseTautulliRecordsJSON(string(body))
	if err != nil {
		return nil, 0, err
	}
	var total int
	var parsed struct {
		Response struct {
			Data struct {
				RecordsFiltered int `json:"recordsFiltered"`
			} `json:"data"`
		} `json:"response"`
	}
	_ = json.Unmarshal(body, &parsed)
	total = parsed.Response.Data.RecordsFiltered
	if total == 0 {
		total = len(records) + start
	}
	return records, total, nil
}

func coerceString(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	default:
		if v == nil {
			return ""
		}
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func coerceInt64(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	case json.Number:
		n, _ := t.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n
	default:
		return 0
	}
}
