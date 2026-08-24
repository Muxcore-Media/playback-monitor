package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

func (m *Module) ImportJellystatHistory(ctx context.Context, req *monitorv1.ImportJellystatHistoryRequest) (*monitorv1.ImportJellystatHistoryResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request required")
	}
	raw := strings.TrimSpace(req.GetBackupJson())
	if raw == "" {
		return nil, fmt.Errorf("backup_json required")
	}
	serverID := strings.TrimSpace(req.GetServerId())
	if serverID == "" {
		serverID = "jellyfin"
	}
	serverType := strings.TrimSpace(req.GetServerType())
	if serverType == "" {
		serverType = "jellyfin"
	}
	maxRecords := int(req.GetMaxRecords())
	if maxRecords <= 0 {
		maxRecords = 50000
	}

	records, err := parseJellystatBackupJSON(raw)
	if err != nil {
		return &monitorv1.ImportJellystatHistoryResponse{Error: err.Error()}, nil //nolint:nilerr // application-level failure encoded in response
	}
	if len(records) > maxRecords {
		records = records[:maxRecords]
	}

	stats, importErr := m.importJellystatRecords(ctx, serverID, serverType, records, req.GetDryRun())
	resp := &monitorv1.ImportJellystatHistoryResponse{
		Imported:     int32(stats.Imported),     //nolint:gosec // import counts are bounded by maxRecords
		Skipped:      int32(stats.Skipped),      //nolint:gosec // import counts are bounded by maxRecords
		Failed:       int32(stats.Failed),       //nolint:gosec // import counts are bounded by maxRecords
		TotalFetched: int32(stats.TotalFetched), //nolint:gosec // import counts are bounded by maxRecords
	}
	if importErr != nil {
		resp.Error = importErr.Error()
	}
	return resp, nil
}

func (m *Module) importJellystatRecords(ctx context.Context, serverID, serverType string, records []map[string]any, dryRun bool) (tautulliImportStats, error) {
	stats := tautulliImportStats{TotalFetched: len(records)}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return stats, fmt.Errorf("db not initialized")
	}
	if err := m.ensureServerRegistered(ctx, db, serverID, serverType, "jellystat-import"); err != nil {
		return stats, err
	}

	batchSeen := map[string]struct{}{}
	for _, rec := range records {
		externalID, ev, startedAt, stoppedAt, ok := jellystatRecordToSessionEvent(rec, serverID, serverType)
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

func jellystatRecordToSessionEvent(rec map[string]any, serverID, serverType string) (externalID string, ev SessionEvent, startedAt, stoppedAt time.Time, ok bool) {
	if rec == nil {
		return "", SessionEvent{}, time.Time{}, time.Time{}, false
	}
	id := coerceString(rec["Id"])
	if id == "" {
		return "", SessionEvent{}, time.Time{}, time.Time{}, false
	}
	externalID = "jellystat:" + id

	durationSec := coerceInt64(rec["PlaybackDuration"])
	if durationSec <= 0 {
		return "", SessionEvent{}, time.Time{}, time.Time{}, false
	}

	stoppedAt = parseISOTime(coerceString(rec["ActivityDateInserted"]))
	if stoppedAt.IsZero() {
		return "", SessionEvent{}, time.Time{}, time.Time{}, false
	}
	startedAt = stoppedAt.Add(-time.Duration(durationSec) * time.Second)

	title := coerceString(rec["NowPlayingItemName"])
	if series := coerceString(rec["SeriesName"]); series != "" {
		title = series + " - " + title
	}
	mediaType := "movie"
	if coerceString(rec["SeriesName"]) != "" {
		mediaType = "episode"
	}

	playMethod := strings.ToLower(coerceString(rec["PlayMethod"]))
	isTranscode := strings.HasPrefix(playMethod, "transcode")

	userName := firstNonEmptyStr(coerceString(rec["UserName"]), coerceString(rec["UserId"]))
	userID := coerceString(rec["UserId"])

	ev = SessionEvent{
		EventType:         playbackevents.EventPlaybackStopped,
		SourceModule:      "jellystat-import",
		ServerID:          serverID,
		ServerType:        serverType,
		ExternalSessionID: externalID,
		UserID:            userID,
		UserName:          userName,
		ItemID:            coerceString(rec["NowPlayingItemId"]),
		Title:             title,
		MediaType:         mediaType,
		PositionSeconds:   durationSec,
		DurationSeconds:   durationSec,
		IsTranscode:       isTranscode,
		Platform:          coerceString(rec["Client"]),
		PlayMethod:        normalizePlayMethod(coerceString(rec["PlayMethod"])),
		Device:            coerceString(rec["DeviceName"]),
		Player:            firstNonEmptyStr(coerceString(rec["DeviceName"]), coerceString(rec["Client"])),
		IPAddress:         coerceString(rec["RemoteEndPoint"]),
		OccurredAt:        stoppedAt,
	}
	return externalID, ev, startedAt, stoppedAt, true
}

func parseJellystatBackupJSON(raw string) ([]map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty backup_json")
	}

	if activities, err := extractJellystatActivities(json.RawMessage(raw)); err == nil && len(activities) > 0 {
		return activities, nil
	}

	var wrapper struct {
		Activities []map[string]any `json:"activities"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err == nil && len(wrapper.Activities) > 0 {
		return wrapper.Activities, nil
	}

	return nil, fmt.Errorf("no jf_playback_activity records found in backup_json")
}

func extractJellystatActivities(raw json.RawMessage) ([]map[string]any, error) {
	var sections []map[string]any
	if err := json.Unmarshal(raw, &sections); err == nil && len(sections) > 0 {
		out := flattenJellystatActivities(sections)
		if len(out) > 0 {
			return out, nil
		}
		if sections[0]["Id"] != nil {
			return sections, nil
		}
	}
	var section map[string]any
	if err := json.Unmarshal(raw, &section); err == nil {
		if section["Id"] != nil {
			return []map[string]any{section}, nil
		}
		out := flattenJellystatActivities([]map[string]any{section})
		if len(out) > 0 {
			return out, nil
		}
	}
	return nil, fmt.Errorf("unrecognized jellystat backup shape")
}

func flattenJellystatActivities(sections []map[string]any) []map[string]any {
	out := make([]map[string]any, 0)
	for _, section := range sections {
		rawList, ok := section["jf_playback_activity"]
		if !ok {
			continue
		}
		items, ok := rawList.([]any)
		if !ok {
			continue
		}
		for _, item := range items {
			if rec, ok := item.(map[string]any); ok {
				out = append(out, rec)
			}
		}
	}
	return out
}

func parseISOTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC()
	}
	return time.Time{}
}
