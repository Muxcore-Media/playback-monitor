package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
	"github.com/google/uuid"
)

type SessionEvent struct {
	EventType          string
	SourceModule       string
	ServerID           string
	ServerType         string
	ExternalSessionID  string
	UserID             string
	UserName           string
	IdentityID         string
	ItemID             string
	MuxcoreID          string
	Title              string
	MediaType          string
	PositionSeconds    int64
	DurationSeconds    int64
	IsPaused           bool
	IsTranscode        bool
	Platform           string
	Device             string
	Player             string
	IPAddress          string
	GeoCountry         string
	GeoCity            string
	GeoLat             float64
	GeoLon             float64
	MediaPath          string
	LibraryName        string
	StreamResolution   string
	PlayMethod         string
	ImdbID             string
	TmdbID             int64
	TvdbID             int64
	OccurredAt         time.Time
}

type SessionRecord struct {
	ID                string
	ServerID          string
	ServerType        string
	ExternalSessionID string
	State             string
	UserID            string
	UserName          string
	ItemID            string
	MuxcoreID         string
	ImdbID            string
	TmdbID            int64
	TvdbID            int64
	Title             string
	MediaType         string
	StartedAt         time.Time
	StoppedAt         time.Time
	LastProgressAt    time.Time
	PositionSeconds   float64
	DurationSeconds   float64
	IsTranscode       bool
	PlayMethod        string
	Platform          string
	Device            string
	Player            string
	IPAddress         string
	GeoCountry        string
	GeoCity           string
	GeoLat            float64
	GeoLon            float64
	SourceModule      string
}

func (m *Module) ingestSessionEvent(ctx context.Context, ev SessionEvent) (sessionID string, created bool, err error) {
	if strings.TrimSpace(ev.EventType) == "" {
		return "", false, fmt.Errorf("event_type required")
	}
	serverID := resolveServerID(ev)
	serverType := strings.TrimSpace(ev.ServerType)
	if serverType == "" {
		serverType = "jellyfin"
	}
	externalID := strings.TrimSpace(ev.ExternalSessionID)
	if externalID == "" {
		externalID = strings.TrimSpace(ev.UserID) + ":" + strings.TrimSpace(ev.ItemID)
	}
	now := ev.OccurredAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	nowStr := now.UTC().Format(time.RFC3339)

	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return "", false, fmt.Errorf("db not initialized")
	}
	if err := m.ensureServerRegistered(ctx, db, serverID, serverType, ev.SourceModule); err != nil {
		return "", false, err
	}
	identityID, err := m.ensureUserIdentity(ctx, db, serverID, ev.UserID, ev.UserName)
	if err != nil {
		return "", false, err
	}
	ev.IdentityID = identityID
	m.enrichSessionGeo(ctx, &ev)
	m.fillSessionExternalIDs(ctx, db, serverID, &ev)

	switch strings.ToLower(ev.EventType) {
	case playbackevents.EventPlaybackStarted:
		id, created, err := m.startSession(ctx, db, ev, serverID, serverType, externalID, nowStr)
		if err == nil {
			_ = m.upsertLibraryItem(ctx, serverID, ev)
			if created {
				go m.firePlaybackNotificationRules(context.Background(), playbackevents.EventPlaybackStarted, ev)
			}
		}
		return id, created, err
	case playbackevents.EventPlaybackProgress:
		id, created, err := m.progressSession(ctx, db, ev, serverID, externalID, nowStr)
		if err == nil {
			_ = m.upsertLibraryItem(ctx, serverID, ev)
		}
		return id, created, err
	case playbackevents.EventPlaybackStopped:
		id, created, err := m.stopSession(ctx, db, ev, serverID, externalID, nowStr)
		if err == nil {
			_ = m.upsertLibraryItem(ctx, serverID, ev)
			go m.notifySessionStop(context.Background(), ev)
		}
		return id, created, err
	default:
		return "", false, fmt.Errorf("unknown event_type %q", ev.EventType)
	}
}

func (m *Module) startSession(ctx context.Context, db *sql.DB, ev SessionEvent, serverID, serverType, externalID, nowStr string) (string, bool, error) {
	var existingID string
	err := m.queryRow(ctx,
		`SELECT id FROM sessions WHERE server_id = ? AND external_session_id = ? AND state IN ('playing','paused') ORDER BY started_at DESC LIMIT 1`,
		serverID, externalID,
	).Scan(&existingID)
	if err == nil && existingID != "" {
		state := "playing"
		if ev.IsPaused {
			state = "paused"
		}
		mediaPath, libraryName := sessionPathFields(ev)
		_, err = m.exec(ctx, `
			UPDATE sessions SET
				state = ?, user_id = ?, user_name = ?, identity_id = COALESCE(NULLIF(?, ''), identity_id),
				item_id = ?, muxcore_id = ?, title = ?,
				media_type = ?, last_progress_at = ?, position_seconds = ?, duration_seconds = ?,
				is_transcode = ?, play_method = COALESCE(NULLIF(?, ''), play_method), platform = ?, device = ?, player = ?, ip_address = ?, source_module = ?,
				media_path = COALESCE(NULLIF(?, ''), media_path),
				library_name = COALESCE(NULLIF(?, ''), library_name),
				stream_resolution = COALESCE(NULLIF(?, ''), stream_resolution),
				`+externalIDUpdateSQL()+`,
				`+geoUpdateClause()+`
			WHERE id = ?`,
			append([]any{
				state, ev.UserID, ev.UserName, ev.IdentityID, ev.ItemID, ev.MuxcoreID, ev.Title, ev.MediaType, nowStr,
				float64(ev.PositionSeconds), float64(ev.DurationSeconds), boolToInt(ev.IsTranscode), playMethodFromEvent(ev),
				ev.Platform, ev.Device, ev.Player, ev.IPAddress, ev.SourceModule,
				mediaPath, libraryName, ev.StreamResolution,
			}, append(externalIDUpdateArgs(ev), append(geoUpdateArgs(ev), existingID)...)...)...,
		)
		return existingID, false, err
	}
	if err != nil && err != sql.ErrNoRows {
		return "", false, err
	}
	mediaPath, libraryName := sessionPathFields(ev)
	id := uuid.NewString()
	state := "playing"
	if ev.IsPaused {
		state = "paused"
	}
	geoCountry, geoCity, geoLat, geoLon := geoInsertValues(ev)
	_, err = m.exec(ctx, `
		INSERT INTO sessions(
			id, server_id, server_type, external_session_id, state, user_id, user_name, identity_id, item_id, muxcore_id,
			title, media_type, started_at, last_progress_at, position_seconds, duration_seconds, is_transcode, play_method,
			platform, device, player, ip_address, source_module, geo_country, geo_city, geo_lat, geo_lon,
			media_path, library_name, stream_resolution, `+sessionExternalIDCols+`
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		append([]any{
			id, serverID, serverType, externalID, state, ev.UserID, ev.UserName, ev.IdentityID, ev.ItemID, ev.MuxcoreID,
			ev.Title, ev.MediaType, nowStr, nowStr, float64(ev.PositionSeconds), float64(ev.DurationSeconds),
			boolToInt(ev.IsTranscode), playMethodFromEvent(ev), ev.Platform, ev.Device, ev.Player, ev.IPAddress, ev.SourceModule,
			geoCountry, geoCity, geoLat, geoLon, mediaPath, libraryName, ev.StreamResolution,
		}, sessionExternalIDInsertArgs(ev)...)...,
	)
	return id, true, err
}

func (m *Module) progressSession(ctx context.Context, db *sql.DB, ev SessionEvent, serverID, externalID, nowStr string) (string, bool, error) {
	var id string
	err := m.queryRow(ctx,
		`SELECT id FROM sessions WHERE server_id = ? AND external_session_id = ? AND state IN ('playing','paused') ORDER BY started_at DESC LIMIT 1`,
		serverID, externalID,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return m.startSession(ctx, db, ev, serverID, strings.TrimSpace(ev.ServerType), externalID, nowStr)
	}
	if err != nil {
		return "", false, err
	}
	mediaPath, libraryName := sessionPathFields(ev)
	state := "playing"
	if ev.IsPaused {
		state = "paused"
	}
	_, err = m.exec(ctx, `
		UPDATE sessions SET
			state = ?, last_progress_at = ?, position_seconds = ?, duration_seconds = ?,
			title = COALESCE(NULLIF(?, ''), title), muxcore_id = COALESCE(NULLIF(?, ''), muxcore_id),
			is_transcode = CASE WHEN ? = 1 THEN 1 ELSE is_transcode END,
			play_method = COALESCE(NULLIF(?, ''), play_method),
			platform = COALESCE(NULLIF(?, ''), platform),
			device = COALESCE(NULLIF(?, ''), device),
			player = COALESCE(NULLIF(?, ''), player),
			ip_address = COALESCE(NULLIF(?, ''), ip_address),
			media_path = COALESCE(NULLIF(?, ''), media_path),
			library_name = COALESCE(NULLIF(?, ''), library_name),
			stream_resolution = COALESCE(NULLIF(?, ''), stream_resolution),
			`+geoUpdateClause()+`
		WHERE id = ?`,
		append([]any{
			state, nowStr, float64(ev.PositionSeconds), float64(ev.DurationSeconds),
			ev.Title, ev.MuxcoreID, boolToInt(ev.IsTranscode), playMethodFromEvent(ev),
			ev.Platform, ev.Device, ev.Player, ev.IPAddress,
			mediaPath, libraryName, ev.StreamResolution,
		}, append(geoUpdateArgs(ev), id)...)...,
	)
	return id, false, err
}

func (m *Module) stopSession(ctx context.Context, db *sql.DB, ev SessionEvent, serverID, externalID, nowStr string) (string, bool, error) {
	var id string
	err := m.queryRow(ctx,
		`SELECT id FROM sessions WHERE server_id = ? AND external_session_id = ? AND state IN ('playing','paused') ORDER BY started_at DESC LIMIT 1`,
		serverID, externalID,
	).Scan(&id)
	if err == sql.ErrNoRows {
		id = uuid.NewString()
		geoCountry, geoCity, geoLat, geoLon := geoInsertValues(ev)
		mediaPath, libraryName := sessionPathFields(ev)
		_, err = m.exec(ctx, `
			INSERT INTO sessions(
				id, server_id, server_type, external_session_id, state, user_id, user_name, identity_id, item_id, muxcore_id,
				title, media_type, started_at, stopped_at, last_progress_at, position_seconds, duration_seconds,
				is_transcode, play_method, platform, device, player, ip_address, source_module, geo_country, geo_city, geo_lat, geo_lon,
				media_path, library_name, stream_resolution, `+sessionExternalIDCols+`
			) VALUES (?, ?, ?, ?, 'stopped', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			append([]any{
				id, serverID, strings.TrimSpace(ev.ServerType), externalID, ev.UserID, ev.UserName, ev.IdentityID, ev.ItemID, ev.MuxcoreID,
				ev.Title, ev.MediaType, nowStr, nowStr, nowStr, float64(ev.PositionSeconds), float64(ev.DurationSeconds),
				boolToInt(ev.IsTranscode), playMethodFromEvent(ev), ev.Platform, ev.Device, ev.Player, ev.IPAddress, ev.SourceModule,
				geoCountry, geoCity, geoLat, geoLon, mediaPath, libraryName, ev.StreamResolution,
			}, sessionExternalIDInsertArgs(ev)...)...,
		)
		return id, true, err
	}
	if err != nil {
		return "", false, err
	}
	_, err = m.exec(ctx, `
		UPDATE sessions SET
			state = 'stopped', stopped_at = ?, last_progress_at = ?,
			position_seconds = CASE WHEN ? > 0 THEN ? ELSE position_seconds END,
			duration_seconds = CASE WHEN ? > 0 THEN ? ELSE duration_seconds END,
			title = COALESCE(NULLIF(?, ''), title),
			stream_resolution = COALESCE(NULLIF(?, ''), stream_resolution)
		WHERE id = ?`,
		nowStr, nowStr,
		ev.PositionSeconds, float64(ev.PositionSeconds),
		ev.DurationSeconds, float64(ev.DurationSeconds),
		ev.Title, ev.StreamResolution, id,
	)
	return id, false, err
}

func (m *Module) listActiveSessions(ctx context.Context, serverID string, limit int) ([]SessionRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	query := `SELECT ` + sessionSelectCols + `
		FROM sessions WHERE state IN ('playing','paused')`
	args := []any{}
	if strings.TrimSpace(serverID) != "" {
		query += ` AND server_id = ?`
		args = append(args, serverID)
	}
	query += ` ORDER BY last_progress_at DESC LIMIT ?`
	args = append(args, limit)
	return m.querySessions(ctx, db, query, args...)
}

func (m *Module) listHistory(ctx context.Context, serverID, userID, q string, limit, offset int) ([]SessionRecord, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, 0, fmt.Errorf("db not initialized")
	}
	where := []string{`state = 'stopped'`}
	args := []any{}
	if strings.TrimSpace(serverID) != "" {
		where = append(where, `server_id = ?`)
		args = append(args, serverID)
	}
	if strings.TrimSpace(userID) != "" {
		where = append(where, `user_id = ?`)
		args = append(args, userID)
	}
	if needle := strings.TrimSpace(q); needle != "" {
		where = append(where, `(title LIKE ? OR user_name LIKE ? OR item_id LIKE ?)`)
		like := "%" + needle + "%"
		args = append(args, like, like, like)
	}
	whereSQL := strings.Join(where, " AND ")
	var total int
	countQuery := `SELECT COUNT(1) FROM sessions WHERE ` + whereSQL
	if err := m.queryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	listQuery := `SELECT ` + sessionSelectCols + `
		FROM sessions WHERE ` + whereSQL + ` ORDER BY started_at DESC LIMIT ? OFFSET ?`
	listArgs := append(append([]any{}, args...), limit, offset)
	rows, err := m.querySessions(ctx, db, listQuery, listArgs...)
	return rows, total, err
}

func (m *Module) querySessions(ctx context.Context, db *sql.DB, query string, args ...any) ([]SessionRecord, error) {
	rs, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	out := make([]SessionRecord, 0)
	for rs.Next() {
		var rec SessionRecord
		var started, stopped, lastProgress string
		var isTranscode int
		if err := rs.Scan(scanSessionRecord(&started, &stopped, &lastProgress, &isTranscode, &rec)...); err != nil {
			return nil, err
		}
		rec.IsTranscode = isTranscode == 1
		rec.StartedAt = parseTime(started)
		rec.StoppedAt = parseTime(stopped)
		rec.LastProgressAt = parseTime(lastProgress)
		out = append(out, rec)
	}
	return out, rs.Err()
}

func (m *Module) homeStats(ctx context.Context, days int) ([]homeStat, error) {
	if days <= 0 {
		days = 30
	}
	since := time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	stats := make([]homeStat, 0, 4)
	var plays int
	if err := m.queryRow(ctx,
		`SELECT COUNT(1) FROM sessions WHERE state = 'stopped' AND started_at >= ?`, since,
	).Scan(&plays); err != nil {
		return nil, err
	}
	stats = append(stats, homeStat{Key: "plays", Label: "Plays", Value: float64(plays)})

	var active int
	if err := m.queryRow(ctx,
		`SELECT COUNT(1) FROM sessions WHERE state IN ('playing','paused')`,
	).Scan(&active); err != nil {
		return nil, err
	}
	stats = append(stats, homeStat{Key: "active_streams", Label: "Active streams", Value: float64(active)})

	var users int
	if err := m.queryRow(ctx,
		`SELECT COUNT(DISTINCT user_id) FROM sessions WHERE state = 'stopped' AND started_at >= ? AND user_id != ''`, since,
	).Scan(&users); err != nil {
		return nil, err
	}
	stats = append(stats, homeStat{Key: "unique_users", Label: "Unique watchers", Value: float64(users)})

	var minutes float64
	if err := m.queryRow(ctx,
		`SELECT COALESCE(SUM(position_seconds), 0) / 60.0 FROM sessions WHERE state = 'stopped' AND started_at >= ?`, since,
	).Scan(&minutes); err != nil {
		return nil, err
	}
	stats = append(stats, homeStat{Key: "watch_minutes", Label: "Watch minutes", Value: minutes})
	return stats, nil
}

type homeStat struct {
	Key   string
	Label string
	Value float64
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func parseTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return t
}
