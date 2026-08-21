package internal

import (
	"fmt"
	"os"

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
	m.cfgMu.RUnlock()
	startVal, stopVal := "0", "0"
	if notifyStart {
		startVal = "1"
	}
	if notifyStop {
		stopVal = "1"
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
	case "notify_on_session_start", "PLAYBACK_MONITOR_NOTIFY_ON_START":
		m.cfgMu.Lock()
		m.notifyOnSessionStart = envTruthy(value)
		m.cfgMu.Unlock()
		return nil
	case "notify_on_session_stop", "PLAYBACK_MONITOR_NOTIFY_ON_STOP":
		m.cfgMu.Lock()
		m.notifyOnSessionStop = envTruthy(value)
		m.cfgMu.Unlock()
		return nil
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
}
