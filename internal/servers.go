package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type serverRecord struct {
	CreatedAt      time.Time
	ID             string
	Name           string
	Type           string
	SourceModule   string
	ActiveSessions int
}

func resolveServerID(ev SessionEvent) string {
	if s := strings.TrimSpace(ev.ServerID); s != "" && s != "default" {
		return s
	}
	if s := strings.TrimSpace(ev.SourceModule); s != "" {
		return s
	}
	if s := strings.TrimSpace(ev.ServerType); s != "" {
		return s
	}
	return "default"
}

func serverDisplayName(serverID, serverType, sourceModule string) string {
	st := strings.TrimSpace(serverType)
	if st == "" {
		st = "server"
	}
	name := strings.ToUpper(st[:1]) + st[1:]
	if src := strings.TrimSpace(sourceModule); src != "" && src != serverID {
		return fmt.Sprintf("%s · %s", name, src)
	}
	if serverID != "" && serverID != "default" {
		return fmt.Sprintf("%s · %s", name, serverID)
	}
	return name
}

func (m *Module) ensureServerRegistered(ctx context.Context, db *sql.DB, serverID, serverType, sourceModule string) error {
	if serverID == "" {
		serverID = "default"
	}
	if serverType == "" {
		serverType = "jellyfin"
	}
	name := serverDisplayName(serverID, serverType, sourceModule)
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := m.exec(ctx, `
		INSERT INTO servers(id, name, type, source_module, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			type = excluded.type,
			source_module = excluded.source_module`,
		serverID, name, serverType, strings.TrimSpace(sourceModule), now,
	)
	return err
}

func (m *Module) registerServer(ctx context.Context, serverID, name, serverType, sourceModule string) (serverRecord, error) {
	if strings.TrimSpace(serverID) == "" {
		return serverRecord{}, fmt.Errorf("server id required")
	}
	_ = name
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return serverRecord{}, fmt.Errorf("db not initialized")
	}
	if err := m.ensureServerRegistered(ctx, db, serverID, serverType, sourceModule); err != nil {
		return serverRecord{}, err
	}
	return m.getServer(ctx, serverID)
}

func (m *Module) listServers(ctx context.Context) ([]serverRecord, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT s.id, s.name, s.type, s.source_module, s.created_at,
			(SELECT COUNT(1) FROM sessions x WHERE x.server_id = s.id AND x.state IN ('playing','paused')) AS active
		FROM servers s
		ORDER BY s.name ASC, s.id ASC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]serverRecord, 0)
	for rows.Next() {
		var rec serverRecord
		var created string
		if err := rows.Scan(&rec.ID, &rec.Name, &rec.Type, &rec.SourceModule, &created, &rec.ActiveSessions); err != nil {
			return nil, err
		}
		rec.CreatedAt = parseTime(created)
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (m *Module) getServer(ctx context.Context, serverID string) (serverRecord, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return serverRecord{}, fmt.Errorf("db not initialized")
	}
	var rec serverRecord
	var created string
	err := m.queryRow(ctx, `
		SELECT s.id, s.name, s.type, s.source_module, s.created_at,
			(SELECT COUNT(1) FROM sessions x WHERE x.server_id = s.id AND x.state IN ('playing','paused')) AS active
		FROM servers s WHERE s.id = ?`, serverID,
	).Scan(&rec.ID, &rec.Name, &rec.Type, &rec.SourceModule, &created, &rec.ActiveSessions)
	if err != nil {
		return serverRecord{}, err
	}
	rec.CreatedAt = parseTime(created)
	return rec, nil
}
