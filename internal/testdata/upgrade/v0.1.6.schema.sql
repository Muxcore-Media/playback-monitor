CREATE TABLE servers (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			type TEXT NOT NULL DEFAULT 'jellyfin',
			source_module TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL
		);
CREATE TABLE sessions (
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
			source_module TEXT NOT NULL DEFAULT '', geo_country TEXT NOT NULL DEFAULT '', geo_city TEXT NOT NULL DEFAULT '', geo_lat REAL NOT NULL DEFAULT 0, geo_lon REAL NOT NULL DEFAULT 0, media_path TEXT NOT NULL DEFAULT '', library_name TEXT NOT NULL DEFAULT '', stream_resolution TEXT NOT NULL DEFAULT '', imdb_id TEXT NOT NULL DEFAULT '', tmdb_id INTEGER NOT NULL DEFAULT 0, tvdb_id INTEGER NOT NULL DEFAULT 0, identity_id TEXT NOT NULL DEFAULT '', play_method TEXT NOT NULL DEFAULT '', kicked INTEGER NOT NULL DEFAULT 0,
			FOREIGN KEY(server_id) REFERENCES servers(id)
		);
CREATE TABLE library_items (
			server_id TEXT NOT NULL,
			item_id TEXT NOT NULL,
			muxcore_id TEXT NOT NULL DEFAULT '',
			title TEXT NOT NULL DEFAULT '',
			media_type TEXT NOT NULL DEFAULT '',
			library_name TEXT NOT NULL DEFAULT '',
			media_path TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL, file_size_bytes INTEGER NOT NULL DEFAULT 0, catalog_added_at TEXT NOT NULL DEFAULT '', removed_at TEXT NOT NULL DEFAULT '', imdb_id TEXT NOT NULL DEFAULT '', tmdb_id INTEGER NOT NULL DEFAULT 0, tvdb_id INTEGER NOT NULL DEFAULT 0, video_resolution TEXT NOT NULL DEFAULT '', parent_id TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (server_id, item_id)
		);
CREATE TABLE user_identities (
			id TEXT PRIMARY KEY,
			display_name TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
CREATE TABLE server_users (
			server_id TEXT NOT NULL,
			external_user_id TEXT NOT NULL,
			identity_id TEXT NOT NULL,
			user_name TEXT NOT NULL DEFAULT '',
			first_seen_at TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			PRIMARY KEY (server_id, external_user_id),
			FOREIGN KEY(identity_id) REFERENCES user_identities(id)
		);
CREATE TABLE library_storage_daily (
			day TEXT NOT NULL,
			server_id TEXT NOT NULL DEFAULT '',
			total_bytes INTEGER NOT NULL DEFAULT 0,
			item_count INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (day, server_id)
		);
CREATE TABLE library_storage_by_library_daily (
			day TEXT NOT NULL,
			server_id TEXT NOT NULL DEFAULT '',
			library_name TEXT NOT NULL DEFAULT '',
			total_bytes INTEGER NOT NULL DEFAULT 0,
			item_count INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (day, server_id, library_name)
		);
CREATE TABLE notification_rules (
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
		);
CREATE TABLE notification_destinations (
		 id TEXT PRIMARY KEY,
		 name TEXT NOT NULL,
		 type TEXT NOT NULL,
		 enabled INTEGER NOT NULL DEFAULT 1,
		 config_json TEXT NOT NULL DEFAULT '{}',
		 events_json TEXT NOT NULL DEFAULT '[]',
		 created_at TEXT NOT NULL,
		 updated_at TEXT NOT NULL
		);
CREATE TABLE request_ready_dispatched (
			request_id TEXT PRIMARY KEY,
			dispatched_at TEXT NOT NULL
		);
CREATE INDEX sessions_active_idx ON sessions(state, last_progress_at);
CREATE INDEX sessions_history_idx ON sessions(started_at DESC);
CREATE INDEX sessions_user_idx ON sessions(user_id, started_at DESC);
CREATE INDEX sessions_external_idx ON sessions(server_id, external_session_id);
CREATE INDEX library_items_library_idx ON library_items(server_id, library_name);
CREATE INDEX server_users_identity_idx ON server_users(identity_id);
CREATE INDEX sessions_identity_idx ON sessions(identity_id, started_at DESC);
CREATE INDEX notification_rules_event_idx ON notification_rules(event_type, enabled);
CREATE INDEX notification_destinations_enabled_idx ON notification_destinations(enabled);
