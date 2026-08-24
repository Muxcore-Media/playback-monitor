package internal

import (
	"context"
	"strings"
)

type streamCategory string

const (
	streamCategoryTranscode    streamCategory = "transcode"
	streamCategoryDirectStream streamCategory = "directStream"
	streamCategoryDirectPlay   streamCategory = "directPlay"
)

func categorizeSession(rec SessionRecord) streamCategory {
	if rec.IsTranscode || strings.EqualFold(strings.TrimSpace(rec.PlayMethod), "transcode") {
		return streamCategoryTranscode
	}
	switch strings.ToLower(strings.TrimSpace(rec.PlayMethod)) {
	case "directstream", "direct stream":
		return streamCategoryDirectStream
	default:
		return streamCategoryDirectPlay
	}
}

func (m *Module) serverNamesByID(ctx context.Context) map[string]string {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return map[string]string{}
	}
	rows, err := m.queryRows(ctx, `SELECT id, name FROM servers`)
	if err != nil {
		return map[string]string{}
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return out
		}
		out[id] = name
	}
	return out
}

func (m *Module) buildStreamSummary(ctx context.Context, rows []SessionRecord) map[string]any {
	names := m.serverNamesByID(ctx)
	transcodes := 0
	directStreams := 0
	directPlays := 0
	summary := map[string]any{
		"total":          len(rows),
		"transcodes":     transcodes,
		"direct_streams": directStreams,
		"direct_plays":   directPlays,
		"by_server":      []map[string]any{},
	}
	type serverAgg struct {
		total         int
		transcodes    int
		directStreams int
		directPlays   int
	}
	byServer := map[string]*serverAgg{}
	for _, rec := range rows {
		cat := categorizeSession(rec)
		switch cat {
		case streamCategoryTranscode:
			transcodes++
		case streamCategoryDirectStream:
			directStreams++
		default:
			directPlays++
		}
		agg := byServer[rec.ServerID]
		if agg == nil {
			agg = &serverAgg{}
			byServer[rec.ServerID] = agg
		}
		agg.total++
		switch cat {
		case streamCategoryTranscode:
			agg.transcodes++
		case streamCategoryDirectStream:
			agg.directStreams++
		default:
			agg.directPlays++
		}
	}
	summary["transcodes"] = transcodes
	summary["direct_streams"] = directStreams
	summary["direct_plays"] = directPlays
	serverRows := make([]map[string]any, 0, len(byServer))
	for serverID, agg := range byServer {
		serverRows = append(serverRows, map[string]any{
			"server_id":      serverID,
			"server_name":    names[serverID],
			"total":          agg.total,
			"transcodes":     agg.transcodes,
			"direct_streams": agg.directStreams,
			"direct_plays":   agg.directPlays,
		})
	}
	summary["by_server"] = serverRows
	return summary
}

func normalizePlayMethod(method string) string {
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "transcode":
		return "Transcode"
	case "directstream", "direct stream":
		return "DirectStream"
	case "directplay", "direct play":
		return "DirectPlay"
	default:
		return strings.TrimSpace(method)
	}
}

func playMethodFromEvent(ev SessionEvent) string {
	if method := normalizePlayMethod(ev.PlayMethod); method != "" {
		return method
	}
	if ev.IsTranscode {
		return "Transcode"
	}
	return ""
}
