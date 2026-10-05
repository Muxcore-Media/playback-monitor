package internal

import (
	"context"
	"fmt"
	"net/http"
	"time"

	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	manifest "github.com/Muxcore-Media/playback-monitor"
)

func (m *Module) handlePublicHealth(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	servers, err := m.listServers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	lastSeen, err := m.serverLastActivity(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]map[string]any, 0, len(servers))
	for _, s := range servers {
		online := true
		if ts, ok := lastSeen[s.ID]; ok {
			online = time.Since(ts) < 7*24*time.Hour
		}
		out = append(out, map[string]any{
			"id":               s.ID,
			"name":             s.Name,
			"type":             s.Type,
			"source_module":    s.SourceModule,
			"online":           online,
			"active_streams":   s.ActiveSessions,
			"last_activity_at": formatOptionalTime(lastSeen[s.ID]),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"version":   modulesdk.ManifestVersion(manifest.ManifestJSON),
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"servers":   out,
	})
}

func (m *Module) handlePublicServers(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	servers, err := m.listServers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := make([]map[string]any, 0, len(servers))
	for _, s := range servers {
		data = append(data, map[string]any{
			"id":              s.ID,
			"name":            s.Name,
			"type":            s.Type,
			"source_module":   s.SourceModule,
			"active_sessions": s.ActiveSessions,
			"created_at":      s.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}

func (m *Module) serverLastActivity(ctx context.Context) (map[string]time.Time, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT server_id, MAX(COALESCE(stopped_at, started_at)) AS last_at
		FROM sessions
		GROUP BY server_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]time.Time{}
	for rows.Next() {
		var serverID, lastAt string
		if err := rows.Scan(&serverID, &lastAt); err != nil {
			return nil, err
		}
		if t := parseTime(lastAt); !t.IsZero() {
			out[serverID] = t
		}
	}
	return out, rows.Err()
}

func formatOptionalTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}
