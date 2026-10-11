package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	monitorv1 "github.com/Muxcore-Media/playback-monitor/proto/monitorv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	// A merge rewrites personal rows and may create the target identity.
	// Exclude both erasure and ingest from before the first lookup through
	// the final commit, so neither endpoint can become stale during the merge.
	// m.mu is released above before acquiring erasureMu, as on ingest/Apply.
	m.erasureMu.Lock()
	defer m.erasureMu.Unlock()
	// This legacy RPC has no server namespace. Refuse a bare id known to be
	// erased even if an external account shares it; ingest keeps its separate
	// server-specific id space. Check before the fallback can create rows.
	for _, id := range []string{sourceID, targetID} {
		if err := m.refuseErased(ctx, nativeServerID, id); err != nil {
			return 0, err
		}
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
	if err := m.refuseErasedMergeIdentities(ctx, sourceIdentity, targetIdentity, sourceID, sourceName); err != nil {
		return 0, err
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

	if _, txErr := m.txExec(ctx, tx, `
		UPDATE server_users SET identity_id = ?, user_name = COALESCE(NULLIF(?, ''), user_name)
		WHERE identity_id = ?`, targetIdentity, targetName, sourceIdentity,
	); txErr != nil {
		return 0, txErr
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
	return int32(n), nil //nolint:gosec // merged session count fits proto int32 field
}

// refuseErasedMergeIdentities covers external aliases of a native account
// whose tombstone was listed but could not yet be applied. Legacy sessions
// may carry a native id without a corresponding server_users link. The
// existing merge UPDATE also matches sessions by source id/name outside the
// resolved identity, so include the native accounts linked to those rows.
// The caller holds erasureMu exclusively across the lookup and the merge.
func (m *Module) refuseErasedMergeIdentities(ctx context.Context, sourceIdentity, targetIdentity, sourceID, sourceName string) error {
	rows, err := m.queryRows(ctx, `
		WITH affected_identities AS (
			SELECT identity_id FROM server_users
			WHERE identity_id != '' AND identity_id IN (?, ?)
			UNION
			SELECT identity_id FROM sessions
			WHERE identity_id != '' AND (
				identity_id IN (?, ?) OR
				(user_id != '' AND user_id = ?) OR
				(user_id = '' AND lower(user_name) = lower(?))
			)
		)
		SELECT external_user_id FROM server_users
		WHERE server_id = ? AND identity_id IN (SELECT identity_id FROM affected_identities)
		UNION
		SELECT user_id FROM sessions
		WHERE server_id = ? AND identity_id IN (SELECT identity_id FROM affected_identities) AND user_id != ''`,
		sourceIdentity, targetIdentity, sourceIdentity, targetIdentity, sourceID, sourceName,
		nativeServerID, nativeServerID,
	)
	if err != nil {
		return err
	}
	var nativeIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return err
		}
		nativeIDs = append(nativeIDs, id)
	}
	iterErr := rows.Err()
	closeErr := rows.Close()
	if iterErr != nil {
		return iterErr
	}
	if closeErr != nil {
		return closeErr
	}
	// Release the query's connection before refuseErased queries the durable
	// record: SQLite deliberately has only one open connection.
	for _, id := range nativeIDs {
		if err := m.refuseErased(ctx, nativeServerID, id); err != nil {
			return err
		}
	}
	return nil
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
	if errors.Is(err, errUserErased) {
		return nil, status.Error(codes.PermissionDenied, errUserErased.Error())
	}
	if err != nil {
		return nil, err
	}
	return &monitorv1.MergeUserIdentityResponse{SessionsUpdated: updated}, nil
}
