package internal

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

func (m *Module) configuredRetentionDays() int {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return m.historyRetentionDays
}

func (m *Module) configuredActiveTimeout() time.Duration {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	if m.activeSessionTimeout <= 0 {
		return 15 * time.Minute
	}
	return m.activeSessionTimeout
}

func (m *Module) historyRetentionLoop(ctx context.Context) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.sweepExpiredHistory(ctx); err != nil {
				slog.Debug("playback-monitor: history retention sweep failed", "error", err)
			}
		}
	}
}

func (m *Module) sweepExpiredHistory(ctx context.Context) error {
	days := m.configuredRetentionDays()
	if days <= 0 {
		return nil
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	res, err := m.exec(ctx, `
		DELETE FROM sessions
		WHERE state = 'stopped' AND stopped_at != '' AND stopped_at < ?`, cutoff)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		slog.Info("playback-monitor: pruned stopped sessions", "count", n, "retention_days", days)
	}
	return nil
}

func (m *Module) deleteUserHistory(ctx context.Context, identityID string) (int64, error) {
	identityID = strings.TrimSpace(identityID)
	if identityID == "" {
		return 0, fmt.Errorf("identity_id required")
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return 0, fmt.Errorf("db not initialized")
	}
	res, err := m.exec(ctx, `DELETE FROM sessions WHERE identity_id = ?`, identityID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (m *Module) DeleteUserHistory(ctx context.Context, req *monitorv1.DeleteUserHistoryRequest) (*monitorv1.DeleteUserHistoryResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request required")
	}
	n, err := m.deleteUserHistory(ctx, req.GetIdentityId())
	if err != nil {
		return nil, err
	}
	return &monitorv1.DeleteUserHistoryResponse{Deleted: n}, nil
}
