package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"database/sql"
)

func (m *Module) initDB(ctx context.Context) error {
	if url := strings.TrimSpace(firstNonEmptyStr(os.Getenv("PLAYBACK_MONITOR_DATABASE_URL"), os.Getenv("DATABASE_URL"))); url != "" &&
		(strings.HasPrefix(url, "postgres://") || strings.HasPrefix(url, "postgresql://")) {
		return m.initPostgresDB(ctx, url)
	}
	return m.initSQLiteDB(ctx)
}

func (m *Module) initSQLiteDB(ctx context.Context) error {
	dir := filepath.Dir(m.dbPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create db dir: %w", err)
	}
	db, err := sql.Open("sqlite", m.dbPath)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	m.mu.Lock()
	m.db = db
	m.dbDialect = dialectSQLite
	m.mu.Unlock()
	if _, err := m.exec(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		_ = db.Close()
		return err
	}
	return m.applySchema(ctx)
}

func (m *Module) initPostgresDB(ctx context.Context, url string) error {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(20)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("postgres ping: %w", err)
	}
	m.mu.Lock()
	m.db = db
	m.dbDialect = dialectPostgres
	m.mu.Unlock()
	if err := m.applySchema(ctx); err != nil {
		return err
	}
	return m.ensureTimescaleHypertable(ctx)
}

func (m *Module) ensureTimescaleHypertable(ctx context.Context) error {
	if strings.TrimSpace(os.Getenv("PLAYBACK_MONITOR_TIMESCALE")) == "" {
		return nil
	}
	_, _ = m.exec(ctx, `CREATE EXTENSION IF NOT EXISTS timescaledb`)
	_, err := m.exec(ctx, `SELECT create_hypertable('sessions', 'started_at', if_not_exists => TRUE)`)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already a hypertable") {
		slog.Warn("timescale hypertable setup skipped", "error", err)
	}
	return nil
}

func (m *Module) applySchema(ctx context.Context) error {
	for _, stmt := range schemaStatements() {
		if _, err := m.exec(ctx, stmt); err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "duplicate column") ||
				strings.Contains(msg, "already exists") ||
				strings.Contains(msg, "duplicate key") {
				continue
			}
		}
	}
	if err := m.ensureDefaultServer(ctx); err != nil {
		return err
	}
	if err := m.backfillUserIdentities(ctx); err != nil {
		return fmt.Errorf("backfill identities: %w", err)
	}
	if err := m.seedDefaultNotificationRules(ctx); err != nil {
		return fmt.Errorf("seed notification rules: %w", err)
	}
	return nil
}

func schemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS servers (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			type TEXT NOT NULL DEFAULT 'jellyfin',
			source_module TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY,
			server_id TEXT NOT NULL DEFAULT 'default',
			server_type TEXT NOT NULL DEFAULT 'jellyfin',
			external_session_id TEXT NOT NULL DEFAULT '',
			state TEXT NOT NULL DEFAULT 'playing',
			user_id TEXT NOT NULL DEFAULT '',
			user_name TEXT NOT NULL DEFAULT '',
			item_id TEXT NOT NULL DEFAULT '',
			muxcore_id TEXT NOT NULL DEFAULT '',
			title TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT '',
			started_at TEXT NOT NULL,
			stopped_at TEXT NOT NULL DEFAULT '',
			last_progress_at TEXT NOT NULL DEFAULT '',
			position_seconds REAL NOT NULL DEFAULT 0,
			duration_seconds REAL NOT NULL DEFAULT 0,
			is_transcode INTEGER NOT NULL DEFAULT 0,
			platform TEXT NOT NULL DEFAULT '',
			device TEXT NOT NULL DEFAULT '',
			player TEXT NOT NULL DEFAULT '',
			ip_address TEXT NOT NULL DEFAULT '',
			source_module TEXT NOT NULL DEFAULT '',
			FOREIGN KEY(server_id) REFERENCES servers(id)
		)`,
		`CREATE INDEX IF NOT EXISTS sessions_active_idx ON sessions(state, last_progress_at)`,
		`CREATE INDEX IF NOT EXISTS sessions_history_idx ON sessions(started_at DESC)`,
		`CREATE INDEX IF NOT EXISTS sessions_user_idx ON sessions(user_id, started_at DESC)`,
		`CREATE INDEX IF NOT EXISTS sessions_external_idx ON sessions(server_id, external_session_id)`,
		`CREATE TABLE IF NOT EXISTS library_items (
			server_id TEXT NOT NULL,
			item_id TEXT NOT NULL,
			muxcore_id TEXT NOT NULL DEFAULT '',
			title TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT '',
			library_name TEXT NOT NULL DEFAULT '',
			media_path TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL,
			PRIMARY KEY (server_id, item_id)
		)`,
		`CREATE INDEX IF NOT EXISTS library_items_library_idx ON library_items(server_id, library_name)`,
		`CREATE TABLE IF NOT EXISTS user_identities (
			id TEXT PRIMARY KEY,
			display_name TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS server_users (
			server_id TEXT NOT NULL,
			external_user_id TEXT NOT NULL,
			identity_id TEXT NOT NULL,
			user_name TEXT NOT NULL DEFAULT '',
			first_seen_at TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			PRIMARY KEY (server_id, external_user_id),
			FOREIGN KEY(identity_id) REFERENCES user_identities(id)
		)`,
		`CREATE INDEX IF NOT EXISTS server_users_identity_idx ON server_users(identity_id)`,
		`CREATE TABLE IF NOT EXISTS library_storage_daily (
			day TEXT NOT NULL,
			server_id TEXT NOT NULL DEFAULT '',
			total_bytes INTEGER NOT NULL DEFAULT 0,
			item_count INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (day, server_id)
		)`,
		`CREATE TABLE IF NOT EXISTS library_storage_by_library_daily (
			day TEXT NOT NULL,
			server_id TEXT NOT NULL DEFAULT '',
			library_name TEXT NOT NULL DEFAULT '',
			total_bytes INTEGER NOT NULL DEFAULT 0,
			item_count INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (day, server_id, library_name)
		)`,
		`ALTER TABLE sessions ADD COLUMN geo_country TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sessions ADD COLUMN geo_city TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sessions ADD COLUMN geo_lat REAL NOT NULL DEFAULT 0`,
		`ALTER TABLE sessions ADD COLUMN geo_lon REAL NOT NULL DEFAULT 0`,
		`ALTER TABLE sessions ADD COLUMN media_path TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sessions ADD COLUMN library_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sessions ADD COLUMN stream_resolution TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE library_items ADD COLUMN file_size_bytes INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE library_items ADD COLUMN catalog_added_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE library_items ADD COLUMN removed_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sessions ADD COLUMN imdb_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sessions ADD COLUMN tmdb_id INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE sessions ADD COLUMN tvdb_id INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE library_items ADD COLUMN imdb_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE library_items ADD COLUMN tmdb_id INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE library_items ADD COLUMN tvdb_id INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE library_items ADD COLUMN video_resolution TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE library_items ADD COLUMN parent_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sessions ADD COLUMN identity_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sessions ADD COLUMN play_method TEXT NOT NULL DEFAULT ''`,
		`CREATE INDEX IF NOT EXISTS sessions_identity_idx ON sessions(identity_id, started_at DESC)`,
		`CREATE TABLE IF NOT EXISTS notification_rules (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			enabled INTEGER NOT NULL DEFAULT 1,
			event_type TEXT NOT NULL,
			title_template TEXT NOT NULL DEFAULT '',
			message_template TEXT NOT NULL DEFAULT '',
			severity TEXT NOT NULL DEFAULT 'info',
			filters_json TEXT NOT NULL DEFAULT '{}',
			destination_ids_json TEXT NOT NULL DEFAULT '[]',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS notification_rules_event_idx ON notification_rules(event_type, enabled)`,
		`ALTER TABLE notification_rules ADD COLUMN destination_ids_json TEXT NOT NULL DEFAULT '[]'`,
		`CREATE TABLE IF NOT EXISTS notification_destinations (
		 id TEXT PRIMARY KEY,
		 name TEXT NOT NULL,
		 type TEXT NOT NULL,
		 enabled INTEGER NOT NULL DEFAULT 1,
		 config_json TEXT NOT NULL DEFAULT '{}',
		 events_json TEXT NOT NULL DEFAULT '[]',
		 created_at TEXT NOT NULL,
		 updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS notification_destinations_enabled_idx ON notification_destinations(enabled)`,
		`CREATE TABLE IF NOT EXISTS request_ready_dispatched (
			request_id TEXT PRIMARY KEY,
			dispatched_at TEXT NOT NULL
		)`,
	}
}

func (m *Module) ensureDefaultServer(ctx context.Context) error {
	var n int
	if err := m.queryRow(ctx, `SELECT COUNT(1) FROM servers`).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := m.exec(ctx,
		`INSERT INTO servers(id, name, type, source_module, created_at) VALUES (?, ?, ?, ?, ?)`,
		"default", "Default Server", "jellyfin", "jellyfin", now,
	)
	return err
}
