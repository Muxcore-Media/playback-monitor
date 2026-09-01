package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type durableSettings struct {
	GeoIPMode            string `json:"geoip_mode"`
	GeoIPDB              string `json:"geoip_db"`
	PublicAPIKey         string `json:"public_api_key"`
	NotifyOnSessionStart bool   `json:"notify_on_session_start"`
	NotifyOnSessionStop  bool   `json:"notify_on_session_stop"`
	HistoryRetentionDays int    `json:"history_retention_days"`
}

func (m *Module) settingsPath() string {
	return filepath.Join(filepath.Dir(m.dbPath), "settings.json")
}

func (m *Module) loadDurableSettings() error {
	path := m.settingsPath()
	data, err := os.ReadFile(path) //nolint:gosec // operator-controlled settings path beside db
	if err != nil {
		if os.IsNotExist(err) {
			return m.persistDurableSettings()
		}
		return err
	}
	var s durableSettings
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	m.notifyOnSessionStart = s.NotifyOnSessionStart
	m.notifyOnSessionStop = s.NotifyOnSessionStop
	if s.GeoIPMode != "" {
		m.geoIPMode = normalizeGeoIPMode(s.GeoIPMode)
	}
	if s.GeoIPDB != "" {
		m.geoIPDBPath = strings.TrimSpace(s.GeoIPDB)
	}
	if s.PublicAPIKey != "" {
		m.publicAPIKey = strings.TrimSpace(s.PublicAPIKey)
	}
	if s.HistoryRetentionDays > 0 {
		m.historyRetentionDays = s.HistoryRetentionDays
	}
	m.reconcileGeoIPEnabledLocked()
	return nil
}

func (m *Module) persistDurableSettings() error {
	m.cfgMu.RLock()
	s := durableSettings{
		NotifyOnSessionStart: m.notifyOnSessionStart,
		NotifyOnSessionStop:  m.notifyOnSessionStop,
		GeoIPMode:            m.geoIPMode,
		GeoIPDB:              m.geoIPDBPath,
		PublicAPIKey:         m.publicAPIKey,
		HistoryRetentionDays: m.historyRetentionDays,
	}
	m.cfgMu.RUnlock()
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	path := m.settingsPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *Module) reconcileGeoIPEnabledLocked() {
	switch m.geoIPMode {
	case "plex":
		m.geoIPEnabled = true
	case "mmdb":
		m.geoIPEnabled = strings.TrimSpace(m.geoIPDBPath) != ""
	default:
		m.geoIPEnabled = strings.TrimSpace(m.geoIPDBPath) != ""
	}
}
