package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
)

func (m *Module) mergeUserIdentities(ctx context.Context, sourceID, sourceName, targetID, targetName string) (int32, error) {
	sourceID = strings.TrimSpace(sourceID)
	sourceName = strings.TrimSpace(sourceName)
	targetID = strings.TrimSpace(targetID)
	targetName = strings.TrimSpace(targetName)
	if sourceID == "" && sourceName == "" {
		return 0, fmt.Errorf("source user required")
	}
	if targetID == "" && targetName == "" {
		return 0, fmt.Errorf("target user required")
	}
	sourceKey := externalUserKey(sourceID, sourceName)
	targetKey := externalUserKey(targetID, targetName)
	if sourceKey == targetKey || (sourceKey == "name:" && targetKey == "name:") {
		return 0, fmt.Errorf("source and target are the same user")
	}

	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return 0, fmt.Errorf("db not initialized")
	}

	sourceIdentity, err := m.lookupIdentityForUser(ctx, db, sourceID, sourceName)
	if err != nil {
		return 0, err
	}
	targetIdentity, err := m.lookupIdentityForUser(ctx, db, targetID, targetName)
	if err != nil {
		return 0, err
	}
	if sourceIdentity == "" {
		return 0, fmt.Errorf("source identity not found")
	}
	if targetIdentity == "" {
		targetIdentity, err = m.ensureUserIdentity(ctx, db, "default", targetID, targetName)
		if err != nil || targetIdentity == "" {
			return 0, fmt.Errorf("target identity required")
		}
	}
	if sourceIdentity == targetIdentity {
		return 0, nil
	}

	now := time.Now().UTC().Format(time.RFC3339)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := m.txExec(ctx, tx, `
		UPDATE server_users SET identity_id = ?, user_name = COALESCE(NULLIF(?, ''), user_name)
		WHERE identity_id = ?`, targetIdentity, targetName, sourceIdentity,
	); err != nil {
		return 0, err
	}

	res, err := m.txExec(ctx, tx, `
		UPDATE sessions SET identity_id = ?, user_id = ?, user_name = ?
		WHERE identity_id = ? OR (
			(user_id != '' AND user_id = ?) OR
			(user_id = '' AND lower(user_name) = lower(?))
		)`, targetIdentity, targetID, targetName, sourceIdentity, sourceID, sourceName,
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()

	if targetName != "" {
		_, _ = m.txExec(ctx, tx, `
			UPDATE user_identities SET display_name = ?, updated_at = ? WHERE id = ?`,
			targetName, now, targetIdentity,
		)
	}
	_, _ = m.txExec(ctx, tx, `DELETE FROM user_identities WHERE id = ?`, sourceIdentity)

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int32(n), nil
}

func (m *Module) lookupIdentityForUser(ctx context.Context, db *sql.DB, userID, userName string) (string, error) {
	extKey := externalUserKey(userID, userName)
	if extKey == "" || extKey == "name:" {
		return "", nil
	}
	var identityID string
	err := m.queryRow(ctx, `
		SELECT identity_id FROM server_users WHERE external_user_id = ?
		ORDER BY last_seen_at DESC LIMIT 1`, extKey,
	).Scan(&identityID)
	if err == sql.ErrNoRows {
		var fromSession string
		err = m.queryRow(ctx, `
			SELECT identity_id FROM sessions
			WHERE identity_id != '' AND (
				(user_id != '' AND user_id = ?) OR
				(user_id = '' AND lower(user_name) = lower(?))
			)
			ORDER BY started_at DESC LIMIT 1`, userID, userName,
		).Scan(&fromSession)
		if err == sql.ErrNoRows {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		return fromSession, nil
	}
	return identityID, err
}

func (m *Module) MergeUserIdentity(ctx context.Context, req *monitorv1.MergeUserIdentityRequest) (*monitorv1.MergeUserIdentityResponse, error) {
	updated, err := m.mergeUserIdentities(ctx, req.GetSourceUserId(), req.GetSourceUserName(), req.GetTargetUserId(), req.GetTargetUserName())
	if err != nil {
		return nil, err
	}
	return &monitorv1.MergeUserIdentityResponse{SessionsUpdated: updated}, nil
}
