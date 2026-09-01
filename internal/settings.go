package internal

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func (m *Module) Settings() []contracts.SettingDef {
	return m.settingsDefs()
}

func (m *Module) UpdateSetting(key, value string) error {
	return m.updateSetting(key, value)
}

func (m *Module) settingsDefs() []contracts.SettingDef {
	m.cfgMu.RLock()
	dbPath := m.dbPath
	notifyStart := m.notifyOnSessionStart
	notifyStop := m.notifyOnSessionStop
	geoMode := m.geoIPMode
	geoDB := m.geoIPDBPath
	publicKey := m.publicAPIKey
	retention := m.historyRetentionDays
	m.cfgMu.RUnlock()
	startVal, stopVal := "0", "0"
	if notifyStart {
		startVal = "1"
	}
	if notifyStop {
		stopVal = "1"
	}
	if geoMode == "" && geoDB != "" {
		geoMode = "mmdb"
	}
	return []contracts.SettingDef{
		{
			Key:         "db_path",
			Label:       "Database Path",
			Type:        contracts.SettingTypeString,
			Value:       dbPath,
			Description: "SQLite path for session history (PLAYBACK_MONITOR_DB_PATH)",
			Required:    false,
			Group:       "Storage",
		},
		{
			Key:         "history_retention_days",
			Label:       "History Retention Days",
			Type:        contracts.SettingTypeString,
			Value:       strconv.Itoa(retention),
			Description: "Delete stopped sessions older than N days (0 keeps forever; PLAYBACK_MONITOR_HISTORY_RETENTION_DAYS)",
			Required:    false,
			Group:       "Storage",
		},
		{
			Key:         "public_api_key",
			Label:       "Public API Key",
			Type:        contracts.SettingTypeString,
			Value:       publicKey,
			Description: "Bearer token for /api/v2/public/* (PLAYBACK_MONITOR_PUBLIC_API_KEY)",
			Required:    false,
			Group:       "API",
		},
		{
			Key:         "geoip_mode",
			Label:       "GeoIP Mode",
			Type:        contracts.SettingTypeString,
			Value:       geoMode,
			Description: "Geo lookup mode: mmdb (local MaxMind DB) or plex (sends client IPs to plex.tv)",
			Required:    false,
			Group:       "GeoIP",
		},
		{
			Key:         "geoip_db",
			Label:       "GeoIP Database Path",
			Type:        contracts.SettingTypeString,
			Value:       geoDB,
			Description: "MaxMind GeoLite2 MMDB path (PLAYBACK_MONITOR_GEOIP_DB); default lookup when set",
			Required:    false,
			Group:       "GeoIP",
		},
		{
			Key:         "notify_on_session_start",
			Label:       "Notify On Session Start",
			Type:        contracts.SettingTypeString,
			Value:       startVal,
			Description: "1 sends notification when playback starts (requires notification module)",
			Required:    false,
			Group:       "Notifications",
		},
		{
			Key:         "notify_on_session_stop",
			Label:       "Notify On Session Stop",
			Type:        contracts.SettingTypeString,
			Value:       stopVal,
			Description: "1 sends notification when playback stops (requires notification module)",
			Required:    false,
			Group:       "Notifications",
		},
	}
}

func (m *Module) updateSetting(key, value string) error {
	switch key {
	case "db_path", "PLAYBACK_MONITOR_DB_PATH":
		m.cfgMu.Lock()
		m.dbPath = value
		m.cfgMu.Unlock()
		if v := os.Getenv("PLAYBACK_MONITOR_DB_PATH"); v == "" {
			_ = os.Setenv("PLAYBACK_MONITOR_DB_PATH", value)
		}
		return fmt.Errorf("db_path change requires module restart")
	case "history_retention_days", "PLAYBACK_MONITOR_HISTORY_RETENTION_DAYS":
		m.cfgMu.Lock()
		m.historyRetentionDays = envIntDefault(value, 0)
		m.cfgMu.Unlock()
		return m.persistDurableSettings()
	case "public_api_key", "PLAYBACK_MONITOR_PUBLIC_API_KEY":
		m.cfgMu.Lock()
		m.publicAPIKey = strings.TrimSpace(value)
		m.cfgMu.Unlock()
		return m.persistDurableSettings()
	case "geoip_mode", "PLAYBACK_MONITOR_GEOIP":
		m.cfgMu.Lock()
		m.geoIPMode = normalizeGeoIPMode(value)
		m.reconcileGeoIPEnabledLocked()
		m.cfgMu.Unlock()
		return m.persistDurableSettings()
	case "geoip_db", "PLAYBACK_MONITOR_GEOIP_DB":
		m.cfgMu.Lock()
		m.geoIPDBPath = strings.TrimSpace(value)
		m.reconcileGeoIPEnabledLocked()
		m.cfgMu.Unlock()
		return m.persistDurableSettings()
	case "notify_on_session_start", "PLAYBACK_MONITOR_NOTIFY_ON_START":
		m.cfgMu.Lock()
		m.notifyOnSessionStart = envTruthy(value)
		m.cfgMu.Unlock()
		return m.persistDurableSettings()
	case "notify_on_session_stop", "PLAYBACK_MONITOR_NOTIFY_ON_STOP":
		m.cfgMu.Lock()
		m.notifyOnSessionStop = envTruthy(value)
		m.cfgMu.Unlock()
		return m.persistDurableSettings()
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
}
