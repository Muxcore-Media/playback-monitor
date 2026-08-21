package internal

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

func (m *Module) IngestSessionEvent(ctx context.Context, req *monitorv1.IngestSessionEventRequest) (*monitorv1.IngestSessionEventResponse, error) {
	ev := sessionEventFromProto(req.GetEvent())
	id, created, err := m.ingestSessionEvent(ctx, ev)
	if err != nil {
		return nil, err
	}
	return &monitorv1.IngestSessionEventResponse{SessionId: id, Created: created}, nil
}

func (m *Module) ListActiveSessions(ctx context.Context, req *monitorv1.ListActiveSessionsRequest) (*monitorv1.ListActiveSessionsResponse, error) {
	rows, err := m.listActiveSessions(ctx, req.GetServerId(), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.SessionRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, sessionRecordToProto(r))
	}
	return &monitorv1.ListActiveSessionsResponse{Sessions: out}, nil
}

func (m *Module) ListHistory(ctx context.Context, req *monitorv1.ListHistoryRequest) (*monitorv1.ListHistoryResponse, error) {
	rows, total, err := m.listHistory(ctx, req.GetServerId(), req.GetUserId(), req.GetQuery(), int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.SessionRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, sessionRecordToProto(r))
	}
	return &monitorv1.ListHistoryResponse{Sessions: out, Total: int32(total)}, nil
}

func (m *Module) GetHomeStats(ctx context.Context, req *monitorv1.GetHomeStatsRequest) (*monitorv1.GetHomeStatsResponse, error) {
	stats, err := m.homeStats(ctx, int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.HomeStat, 0, len(stats))
	for _, s := range stats {
		out = append(out, &monitorv1.HomeStat{Key: s.Key, Label: s.Label, Value: s.Value})
	}
	return &monitorv1.GetHomeStatsResponse{Stats: out}, nil
}

func (m *Module) GetItemWatchStats(ctx context.Context, req *monitorv1.GetItemWatchStatsRequest) (*monitorv1.GetItemWatchStatsResponse, error) {
	ws, err := m.itemWatchStats(ctx, req.GetItemId(), int(req.GetRuntimeMinutes()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetItemWatchStatsResponse{
		ViewCount:                 int32(ws.ViewCount),
		PlayCount:                 int32(ws.PlayCount),
		UniqueUserCount:           int32(ws.UniqueUsers),
		TotalDurationMinutes:      ws.TotalDurationMinutes,
		LongestDurationMinutes:    ws.LongestDurationMinutes,
		HasActivity:               ws.HasActivity,
		NeverWatched:              ws.NeverWatched,
		DaysSinceLastWatch:        int32(ws.DaysSinceLastWatch),
		LastWatchedAtUnix:         ws.LastWatchedAt.Unix(),
		UserWatchedPercent:        ws.UserWatchedPercent,
		UserWatchedDurationMinutes: ws.UserDurationMinutes,
	}, nil
}

func (m *Module) ListWatchUsers(ctx context.Context, req *monitorv1.ListWatchUsersRequest) (*monitorv1.ListWatchUsersResponse, error) {
	users, err := m.listWatchUsers(ctx, req.GetQuery(), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.WatchUser, 0, len(users))
	for _, u := range users {
		out = append(out, &monitorv1.WatchUser{Username: u})
	}
	return &monitorv1.ListWatchUsersResponse{Users: out}, nil
}

func (m *Module) GetStreamAnalytics(ctx context.Context, req *monitorv1.GetStreamAnalyticsRequest) (*monitorv1.GetStreamAnalyticsResponse, error) {
	platforms, transcodes, err := m.streamAnalytics(ctx, int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	resp := &monitorv1.GetStreamAnalyticsResponse{}
	for _, row := range platforms {
		resp.Platforms = append(resp.Platforms, &monitorv1.BreakdownRow{Key: row.Key, Label: row.Label, Count: int32(row.Count)})
	}
	for _, row := range transcodes {
		resp.Transcodes = append(resp.Transcodes, &monitorv1.BreakdownRow{Key: row.Key, Label: row.Label, Count: int32(row.Count)})
	}
	return resp, nil
}

func (m *Module) ListUserWatchStats(ctx context.Context, req *monitorv1.ListUserWatchStatsRequest) (*monitorv1.ListUserWatchStatsResponse, error) {
	users, err := m.listUserWatchStats(ctx, int(req.GetDays()), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.UserWatchStat, 0, len(users))
	for _, u := range users {
		out = append(out, &monitorv1.UserWatchStat{
			Username:     u.Username,
			PlayCount:    int32(u.PlayCount),
			WatchMinutes: u.WatchMinutes,
		})
	}
	return &monitorv1.ListUserWatchStatsResponse{Users: out}, nil
}

func (m *Module) ListServers(ctx context.Context, _ *monitorv1.ListServersRequest) (*monitorv1.ListServersResponse, error) {
	servers, err := m.listServers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.ServerRecord, 0, len(servers))
	for _, s := range servers {
		out = append(out, &monitorv1.ServerRecord{
			Id:             s.ID,
			Name:           s.Name,
			Type:           s.Type,
			SourceModule:   s.SourceModule,
			ActiveSessions: int32(s.ActiveSessions),
			CreatedAtUnix:  s.CreatedAt.Unix(),
		})
	}
	return &monitorv1.ListServersResponse{Servers: out}, nil
}

func (m *Module) RegisterServer(ctx context.Context, req *monitorv1.RegisterServerRequest) (*monitorv1.RegisterServerResponse, error) {
	rec, err := m.registerServer(ctx, req.GetId(), req.GetName(), req.GetType(), req.GetSourceModule())
	if err != nil {
		return nil, err
	}
	return &monitorv1.RegisterServerResponse{
		Server: &monitorv1.ServerRecord{
			Id:             rec.ID,
			Name:           rec.Name,
			Type:           rec.Type,
			SourceModule:   rec.SourceModule,
			ActiveSessions: int32(rec.ActiveSessions),
			CreatedAtUnix:  rec.CreatedAt.Unix(),
		},
	}, nil
}

func (m *Module) GetPlaysByDate(ctx context.Context, req *monitorv1.GetPlaysByDateRequest) (*monitorv1.GetPlaysByDateResponse, error) {
	userIDs, err := parseChartUserIDs(req.GetUserId())
	if err != nil {
		return nil, err
	}
	if req.GetGrouping() || len(userIDs) > 0 || strings.EqualFold(req.GetYAxis(), "duration") {
		chart, err := m.playsByDateChart(ctx, playsByDateChartOpts{
			Days:     int(req.GetDays()),
			UserIDs:  userIDs,
			Grouping: req.GetGrouping(),
			YAxis:    req.GetYAxis(),
		})
		if err != nil {
			return nil, err
		}
		return playsByDateChartToProto(chart), nil
	}
	rows, err := m.playsByDate(ctx, int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.PlaysByDateRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, &monitorv1.PlaysByDateRow{Date: row.Date, Count: int32(row.Count)})
	}
	return &monitorv1.GetPlaysByDateResponse{Rows: out}, nil
}

func playsByDateChartToProto(chart playsByDateChart) *monitorv1.GetPlaysByDateResponse {
	out := &monitorv1.GetPlaysByDateResponse{Categories: chart.Categories}
	for _, s := range chart.Series {
		data := make([]int32, len(s.Data))
		for i, v := range s.Data {
			data[i] = int32(v)
		}
		out.Series = append(out.Series, &monitorv1.PlaysByDateSeries{Name: s.Name, Data: data})
	}
	return out
}

func (m *Module) GetPlaysByHour(ctx context.Context, req *monitorv1.GetPlaysByHourRequest) (*monitorv1.GetPlaysByHourResponse, error) {
	rows, err := m.playsByHour(ctx, int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetPlaysByHourResponse{Rows: chartBucketsToProto(rows)}, nil
}

func (m *Module) GetPlaysByDayOfWeek(ctx context.Context, req *monitorv1.GetPlaysByDayOfWeekRequest) (*monitorv1.GetPlaysByDayOfWeekResponse, error) {
	rows, err := m.playsByDayOfWeek(ctx, int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetPlaysByDayOfWeekResponse{Rows: chartBucketsToProto(rows)}, nil
}

func (m *Module) GetPlaysByMonth(ctx context.Context, req *monitorv1.GetPlaysByMonthRequest) (*monitorv1.GetPlaysByMonthResponse, error) {
	rows, err := m.playsByMonth(ctx, int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetPlaysByMonthResponse{Rows: chartBucketsToProto(rows)}, nil
}

func (m *Module) GetPlaysByStreamType(ctx context.Context, req *monitorv1.GetPlaysByStreamTypeRequest) (*monitorv1.GetPlaysByStreamTypeResponse, error) {
	rows, err := m.playsByStreamType(ctx, int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetPlaysByStreamTypeResponse{Rows: chartBucketsToProto(rows)}, nil
}

func (m *Module) GetPlaysByStreamResolution(ctx context.Context, req *monitorv1.GetPlaysByStreamResolutionRequest) (*monitorv1.GetPlaysByStreamResolutionResponse, error) {
	rows, err := m.playsByStreamResolution(ctx, int(req.GetDays()), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetPlaysByStreamResolutionResponse{Rows: chartBucketsToProto(rows)}, nil
}

func (m *Module) GetPlaysBySourceResolution(ctx context.Context, req *monitorv1.GetPlaysBySourceResolutionRequest) (*monitorv1.GetPlaysBySourceResolutionResponse, error) {
	rows, err := m.playsBySourceResolution(ctx, int(req.GetDays()), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetPlaysBySourceResolutionResponse{Rows: chartBucketsToProto(rows)}, nil
}

func (m *Module) GetPlaysByPlatformResolution(ctx context.Context, req *monitorv1.GetPlaysByPlatformResolutionRequest) (*monitorv1.GetPlaysByPlatformResolutionResponse, error) {
	rows, err := m.playsByPlatformResolution(ctx, int(req.GetDays()), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetPlaysByPlatformResolutionResponse{Rows: chartBucketsToProto(rows)}, nil
}

func (m *Module) GetPlaysByTopUsers(ctx context.Context, req *monitorv1.GetPlaysByTopUsersRequest) (*monitorv1.GetPlaysByTopUsersResponse, error) {
	rows, err := m.playsByTopUsers(ctx, int(req.GetDays()), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetPlaysByTopUsersResponse{Rows: chartBucketsToProto(rows)}, nil
}

func (m *Module) GetPlaysByTopPlatforms(ctx context.Context, req *monitorv1.GetPlaysByTopPlatformsRequest) (*monitorv1.GetPlaysByTopPlatformsResponse, error) {
	rows, err := m.playsByTopPlatforms(ctx, int(req.GetDays()), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.GetPlaysByTopPlatformsResponse{Rows: chartBucketsToProto(rows)}, nil
}

func (m *Module) GetConcurrentStreams(ctx context.Context, req *monitorv1.GetConcurrentStreamsRequest) (*monitorv1.GetConcurrentStreamsResponse, error) {
	rows, err := m.concurrentStreamRows(ctx, int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.ConcurrentStreamRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, &monitorv1.ConcurrentStreamRow{
			Date:      row.Date,
			Total:     int32(row.Total),
			Direct:    int32(row.Direct),
			Transcode: int32(row.Transcode),
		})
	}
	return &monitorv1.GetConcurrentStreamsResponse{Rows: out}, nil
}

func chartBucketsToProto(rows []chartBucketRow) []*monitorv1.ChartBucketRow {
	out := make([]*monitorv1.ChartBucketRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, &monitorv1.ChartBucketRow{
			Key:   row.Key,
			Label: row.Label,
			Count: int32(row.Count),
		})
	}
	return out
}

func (m *Module) ListLibraryStats(ctx context.Context, req *monitorv1.ListLibraryStatsRequest) (*monitorv1.ListLibraryStatsResponse, error) {
	rows, err := m.listLibraryStats(ctx, int(req.GetDays()), req.GetServerId(), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.LibraryStat, 0, len(rows))
	for _, row := range rows {
		out = append(out, &monitorv1.LibraryStat{
			ServerId:     row.ServerID,
			LibraryName:  row.LibraryName,
			PlayCount:    int32(row.PlayCount),
			WatchMinutes: row.WatchMinutes,
		})
	}
	return &monitorv1.ListLibraryStatsResponse{Libraries: out}, nil
}

func (m *Module) ListLibraryDuplicates(ctx context.Context, req *monitorv1.ListLibraryDuplicatesRequest) (*monitorv1.ListLibraryDuplicatesResponse, error) {
	groups, err := m.listLibraryDuplicates(ctx, req.GetServerId(), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.LibraryDuplicateGroup, 0, len(groups))
	for _, g := range groups {
		copies := make([]*monitorv1.LibraryDuplicateCopy, 0, len(g.Copies))
		for _, c := range g.Copies {
			copies = append(copies, &monitorv1.LibraryDuplicateCopy{
				ServerId:      c.ServerID,
				ItemId:        c.ItemID,
				Title:         c.Title,
				LibraryName:   c.LibraryName,
				MediaPath:     c.MediaPath,
				FileSizeBytes: c.FileSizeBytes,
			})
		}
		out = append(out, &monitorv1.LibraryDuplicateGroup{
			GroupKey:  g.GroupKey,
			Title:     g.Title,
			CopyCount: int32(g.CopyCount),
			Copies:    copies,
		})
	}
	return &monitorv1.ListLibraryDuplicatesResponse{Groups: out}, nil
}

func (m *Module) ListStaleLibraryItems(ctx context.Context, req *monitorv1.ListStaleLibraryItemsRequest) (*monitorv1.ListStaleLibraryItemsResponse, error) {
	items, never, stale, err := m.listStaleLibraryItems(ctx, req.GetServerId(), int(req.GetStaleDays()), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	out := make([]*monitorv1.StaleLibraryItem, 0, len(items))
	for _, item := range items {
		lastWatched := ""
		if item.LastWatched != nil {
			lastWatched = item.LastWatched.UTC().Format(time.RFC3339)
		}
		out = append(out, &monitorv1.StaleLibraryItem{
			ServerId:      item.ServerID,
			ItemId:        item.ItemID,
			Title:         item.Title,
			MediaType:     item.MediaType,
			LibraryName:   item.LibraryName,
			FileSizeBytes: item.FileSizeBytes,
			LastWatched:   lastWatched,
			WatchCount:    int32(item.WatchCount),
			Category:      item.Category,
			DaysStale:     int32(item.DaysStale),
		})
	}
	return &monitorv1.ListStaleLibraryItemsResponse{
		Items:             out,
		NeverWatchedCount: int32(never),
		StaleCount:        int32(stale),
	}, nil
}

func (m *Module) GetLibraryStorageSummary(ctx context.Context, req *monitorv1.GetLibraryStorageSummaryRequest) (*monitorv1.GetLibraryStorageSummaryResponse, error) {
	summary, err := m.getLibraryStorageSummary(ctx, req.GetServerId())
	if err != nil {
		return nil, err
	}
	libraries := make([]*monitorv1.LibraryStorageRow, 0, len(summary.Libraries))
	for _, row := range summary.Libraries {
		libraries = append(libraries, &monitorv1.LibraryStorageRow{
			ServerId:    row.ServerID,
			LibraryName: row.LibraryName,
			ItemCount:   int32(row.ItemCount),
			TotalBytes:  row.TotalBytes,
		})
	}
	return &monitorv1.GetLibraryStorageSummaryResponse{
		TotalItems:           int32(summary.TotalItems),
		TotalBytes:           summary.TotalBytes,
		DuplicateWasteBytes:  summary.DuplicateWaste,
		Libraries:            libraries,
	}, nil
}

func (m *Module) GetLibraryStorageHistory(ctx context.Context, req *monitorv1.GetLibraryStorageHistoryRequest) (*monitorv1.GetLibraryStorageHistoryResponse, error) {
	points, err := m.getLibraryStorageHistory(ctx, req.GetServerId(), req.GetLibraryName(), int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	history := make([]*monitorv1.LibraryStorageHistoryPoint, 0, len(points))
	for _, p := range points {
		history = append(history, &monitorv1.LibraryStorageHistoryPoint{
			Day:        p.Day,
			TotalBytes: p.TotalBytes,
			ItemCount:  int32(p.ItemCount),
		})
	}
	predictDays := int(req.GetPredictDays())
	if predictDays <= 0 {
		predictDays = 90
	}
	pred := predictLibraryStorageGrowth(points, predictDays)
	return &monitorv1.GetLibraryStorageHistoryResponse{
		History: history,
		Prediction: &monitorv1.LibraryStoragePrediction{
			GrowthBytesPerDay: pred.GrowthBytesPerDay,
			ProjectedBytes:    pred.ProjectedBytes,
			HorizonDays:       int32(pred.HorizonDays),
		},
	}, nil
}

func (m *Module) ListTopContent(ctx context.Context, req *monitorv1.ListTopContentRequest) (*monitorv1.ListTopContentResponse, error) {
	movies, shows, other, err := m.listTopContent(ctx, int(req.GetDays()), req.GetServerId(), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	return &monitorv1.ListTopContentResponse{
		Movies: topContentToProto(movies),
		Shows:  topContentToProto(shows),
		Other:  topContentToProto(other),
	}, nil
}

func topContentToProto(rows []topContentRow) []*monitorv1.TopContentRow {
	out := make([]*monitorv1.TopContentRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, &monitorv1.TopContentRow{
			Title:        row.Title,
			MediaType:    row.MediaType,
			PlayCount:    int32(row.PlayCount),
			WatchMinutes: row.WatchMinutes,
		})
	}
	return out
}

func sessionEventFromProto(ev *monitorv1.SessionEvent) SessionEvent {
	if ev == nil {
		return SessionEvent{}
	}
	var occurred time.Time
	if ev.GetOccurredAtUnix() > 0 {
		occurred = time.Unix(ev.GetOccurredAtUnix(), 0).UTC()
	}
	return SessionEvent{
		EventType:         ev.GetEventType(),
		SourceModule:      ev.GetSourceModule(),
		ServerID:          ev.GetServerId(),
		ServerType:        ev.GetServerType(),
		ExternalSessionID: ev.GetExternalSessionId(),
		UserID:            ev.GetUserId(),
		UserName:          ev.GetUserName(),
		ItemID:            ev.GetItemId(),
		MuxcoreID:         ev.GetMuxcoreId(),
		Title:             ev.GetTitle(),
		MediaType:         ev.GetMediaType(),
		PositionSeconds:   ev.GetPositionSeconds(),
		DurationSeconds:   ev.GetDurationSeconds(),
		IsPaused:          ev.GetIsPaused(),
		IsTranscode:       ev.GetIsTranscode(),
		Platform:          ev.GetPlatform(),
		Device:            ev.GetDevice(),
		Player:            ev.GetPlayer(),
		IPAddress:         ev.GetIpAddress(),
		MediaPath:         ev.GetMediaPath(),
		OccurredAt:        occurred,
	}
}

func sessionRecordToProto(r SessionRecord) *monitorv1.SessionRecord {
	return &monitorv1.SessionRecord{
		Id:                r.ID,
		ServerId:          r.ServerID,
		ServerType:        r.ServerType,
		ExternalSessionId: r.ExternalSessionID,
		State:             sessionStateToProto(r.State),
		UserId:            r.UserID,
		UserName:          r.UserName,
		ItemId:            r.ItemID,
		MuxcoreId:         r.MuxcoreID,
		Title:             r.Title,
		MediaType:         r.MediaType,
		StartedAtUnix:     r.StartedAt.Unix(),
		StoppedAtUnix:     r.StoppedAt.Unix(),
		LastProgressAtUnix: r.LastProgressAt.Unix(),
		PositionSeconds:   int64(r.PositionSeconds),
		DurationSeconds:   int64(r.DurationSeconds),
		IsTranscode:       r.IsTranscode,
		Platform:          r.Platform,
		Device:            r.Device,
		Player:            r.Player,
		IpAddress:         r.IPAddress,
		SourceModule:      r.SourceModule,
		GeoCountry:        r.GeoCountry,
		GeoCity:           r.GeoCity,
		GeoLat:            r.GeoLat,
		GeoLon:            r.GeoLon,
	}
}

func sessionStateToProto(state string) monitorv1.SessionState {
	switch strings.ToLower(state) {
	case "playing":
		return monitorv1.SessionState_SESSION_STATE_PLAYING
	case "paused":
		return monitorv1.SessionState_SESSION_STATE_PAUSED
	case "stopped":
		return monitorv1.SessionState_SESSION_STATE_STOPPED
	default:
		return monitorv1.SessionState_SESSION_STATE_UNSPECIFIED
	}
}

func sessionEventFromPlaybackJSON(sourceModule string, payload []byte) (SessionEvent, error) {
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return SessionEvent{}, err
	}
	ev := SessionEvent{
		SourceModule: sourceModule,
		ServerID:     "default",
		ServerType:   inferServerType(sourceModule),
	}
	if s := stringField(raw, "source_module", "sourceModule"); s != "" {
		ev.SourceModule = s
	}
	if s := stringField(raw, "server_id", "serverId"); s != "" {
		ev.ServerID = s
	}
	if s := stringField(raw, "server_type", "serverType"); s != "" {
		ev.ServerType = s
	}
	if s, ok := raw["event_type"].(string); ok {
		ev.EventType = s
	}
	if s, ok := raw["notification_type"].(string); ok && ev.EventType == "" {
		switch strings.ToLower(s) {
		case "playbackstart":
			ev.EventType = "playback.started"
		case "playbackprogress":
			ev.EventType = "playback.progress"
		case "playbackstop":
			ev.EventType = "playback.stopped"
		}
	}
	ev.ExternalSessionID = firstNonEmpty(
		stringField(raw, "external_session_id", "externalSessionId"),
		stringField(raw, "session_id", "SessionId", "SessionID"),
	)
	ev.UserID = stringField(raw, "user_id", "UserId", "UserID")
	ev.UserName = stringField(raw, "user_name", "UserName", "NotificationUsername")
	ev.ItemID = firstNonEmpty(stringField(raw, "item_id", "ItemId", "ItemID"), stringField(raw, "jellyfin_item_id"))
	ev.MuxcoreID = stringField(raw, "muxcore_id")
	ev.Title = stringField(raw, "title", "Title", "Name")
	ev.MediaPath = stringField(raw, "media_path", "Path")
	ev.LibraryName = stringField(raw, "library_name", "LibraryName")
	ev.Platform = stringField(raw, "platform", "Platform")
	ev.Device = stringField(raw, "device", "Device")
	ev.Player = stringField(raw, "player", "Player")
	ev.IPAddress = stringField(raw, "ip_address", "remote_ip", "RemoteAddress")
	ev.StreamResolution = stringField(raw, "stream_resolution", "streamResolution", "video_resolution", "videoResolution")
	ev.ImdbID, ev.TmdbID, ev.TvdbID = externalIDsFromMap(raw)
	if ev.StreamResolution == "" {
		height := int(int64Field(raw, "video_height", "videoHeight", "height", "Height"))
		width := int(int64Field(raw, "video_width", "videoWidth", "width", "Width"))
		ev.StreamResolution = normalizeStreamResolution(height, width, "")
	}
	if v, ok := raw["is_paused"].(bool); ok {
		ev.IsPaused = v
	}
	if v, ok := raw["is_transcode"].(bool); ok {
		ev.IsTranscode = v
	}
	ev.PlayMethod = normalizePlayMethod(stringField(raw, "play_method", "playMethod", "PlayMethod"))
	if ev.PlayMethod == "" && ev.IsTranscode {
		ev.PlayMethod = "Transcode"
	}
	ev.PositionSeconds = int64Field(raw, "position_seconds", "PositionSeconds")
	ev.DurationSeconds = int64Field(raw, "duration_seconds", "DurationSeconds")
	if ev.PositionSeconds == 0 {
		ticks := int64Field(raw, "position_ticks", "PositionTicks", "PlaybackPositionTicks")
		if ticks > 0 {
			ev.PositionSeconds = ticks / 10_000_000
		}
	}
	if ev.DurationSeconds == 0 {
		ticks := int64Field(raw, "duration_ticks", "DurationTicks", "RunTimeTicks")
		if ticks > 0 {
			ev.DurationSeconds = ticks / 10_000_000
		}
	}
	if u := int64Field(raw, "occurred_at_unix", "occurredAtUnix"); u > 0 {
		ev.OccurredAt = time.Unix(u, 0).UTC()
	}
	return ev, nil
}

func inferServerType(sourceModule string) string {
	switch strings.ToLower(strings.TrimSpace(sourceModule)) {
	case "plex":
		return "plex"
	case "emby":
		return "emby"
	default:
		return "jellyfin"
	}
}

func stringField(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

func int64Field(m map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch t := v.(type) {
			case float64:
				return int64(t)
			case int64:
				return t
			case json.Number:
				n, _ := t.Int64()
				return n
			case string:
				if n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64); err == nil {
					return n
				}
			}
		}
	}
	return 0
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
