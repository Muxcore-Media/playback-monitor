package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Tracearr-style stable UUID identities: one identity spans server_user accounts.
func externalUserKey(userID, userName string) string {
	userID = strings.TrimSpace(userID)
	if userID != "" {
		return userID
	}
	return "name:" + strings.ToLower(strings.TrimSpace(userName))
}

func (m *Module) ensureUserIdentity(ctx context.Context, db *sql.DB, serverID, userID, userName string) (string, error) {
	serverID = strings.TrimSpace(serverID)
	extKey := externalUserKey(userID, userName)
	if serverID == "" || extKey == "" || extKey == "name:" {
		return "", nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	displayName := strings.TrimSpace(userName)
	if displayName == "" {
		displayName = extKey
	}

	var identityID string
	err := m.queryRow(ctx,
		`SELECT identity_id FROM server_users WHERE server_id = ? AND external_user_id = ?`,
		serverID, extKey,
	).Scan(&identityID)
	if err == nil && identityID != "" {
		_, _ = m.exec(ctx, `
			UPDATE server_users SET user_name = COALESCE(NULLIF(?, ''), user_name), last_seen_at = ?
			WHERE server_id = ? AND external_user_id = ?`,
			userName, now, serverID, extKey,
		)
		_, _ = m.exec(ctx, `
			UPDATE user_identities SET display_name = COALESCE(NULLIF(?, ''), display_name), updated_at = ?
			WHERE id = ?`, displayName, now, identityID,
		)
		return identityID, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}

	identityID = uuid.NewString()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := m.txExec(ctx, tx, `
		INSERT INTO user_identities(id, display_name, created_at, updated_at)
		VALUES (?, ?, ?, ?)`, identityID, displayName, now, now,
	); err != nil {
		return "", err
	}
	if _, err := m.txExec(ctx, tx, `
		INSERT INTO server_users(server_id, external_user_id, identity_id, user_name, first_seen_at, last_seen_at)
		VALUES (?, ?, ?, ?, ?, ?)`, serverID, extKey, identityID, userName, now, now,
	); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return identityID, nil
}

func (m *Module) backfillUserIdentities(ctx context.Context) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT DISTINCT server_id, user_id, user_name
		FROM sessions
		WHERE (user_id != '' OR user_name != '')
		  AND (identity_id = '' OR identity_id IS NULL)`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	type row struct {
		serverID, userID, userName string
	}
	pending := make([]row, 0)
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.serverID, &r.userID, &r.userName); err != nil {
			return err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, r := range pending {
		identityID, err := m.ensureUserIdentity(ctx, db, r.serverID, r.userID, r.userName)
		if err != nil || identityID == "" {
			continue
		}
		_, _ = m.exec(ctx, `
			UPDATE sessions SET identity_id = ?
			WHERE server_id = ? AND identity_id = '' AND (
				(user_id != '' AND user_id = ?) OR
				(user_id = '' AND lower(user_name) = lower(?))
			)`, identityID, r.serverID, r.userID, r.userName,
		)
	}
	return nil
}

func (m *Module) resolveIdentityID(ctx context.Context, rawID string) (identityID, userID, userName string, err error) {
	rawID = strings.TrimSpace(rawID)
	if rawID == "" {
		return "", "", "", fmt.Errorf("identity id required")
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return "", "", "", fmt.Errorf("db not initialized")
	}

	// Tracearr public API uses stable UUID identities.
	if _, parseErr := uuid.Parse(rawID); parseErr == nil {
		var displayName string
		err = m.queryRow(ctx,
			`SELECT display_name FROM user_identities WHERE id = ?`, rawID,
		).Scan(&displayName)
		if err == nil {
			return rawID, "", displayName, nil
		}
		if err != sql.ErrNoRows {
			return "", "", "", err
		}
	}

	// Legacy lookup: external user id or name: prefix.
	uid, uname, parseErr := parsePublicUserIdentityID(rawID)
	if parseErr != nil {
		return "", "", "", parseErr
	}
	extKey := externalUserKey(uid, uname)
	var foundID string
	err = m.queryRow(ctx, `
		SELECT identity_id FROM server_users WHERE external_user_id = ?
		ORDER BY last_seen_at DESC LIMIT 1`, extKey,
	).Scan(&foundID)
	if err == sql.ErrNoRows {
		return rawID, uid, uname, nil
	}
	if err != nil {
		return "", "", "", err
	}
	var displayName string
	_ = m.queryRow(ctx, `SELECT display_name FROM user_identities WHERE id = ?`, foundID).Scan(&displayName)
	if displayName != "" {
		uname = displayName
	}
	return foundID, uid, uname, nil
}

func (m *Module) listServerUsersForIdentity(ctx context.Context, identityID string) ([]publicUserAccount, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT su.server_id, COALESCE(s.type, ''), su.external_user_id, su.user_name
		FROM server_users su
		LEFT JOIN servers s ON s.id = su.server_id
		WHERE su.identity_id = ?
		ORDER BY su.server_id ASC`, identityID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]publicUserAccount, 0)
	for rows.Next() {
		var acc publicUserAccount
		if err := rows.Scan(&acc.ServerID, &acc.ServerType, &acc.ExternalUserID, &acc.Username); err != nil {
			return nil, err
		}
		out = append(out, acc)
	}
	return out, rows.Err()
}

func identityMatchSQL(alias string) string {
	return fmt.Sprintf(`%[1]sidentity_id = ?`, alias)
}

func parsePublicUserIdentityID(raw string) (userID, userName string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("user id required")
	}
	if strings.HasPrefix(raw, "name:") {
		return "", strings.TrimPrefix(raw, "name:"), nil
	}
	return raw, "", nil
}

func userIdentityMatchSQL(alias string) string {
	return fmt.Sprintf(`(
		(%[1]suser_id != '' AND %[1]suser_id = ?) OR
		(%[1]suser_id = '' AND lower(%[1]suser_name) = lower(?))
	)`, alias)
}

func encodeUserCursor(lastSeen time.Time, id string) string {
	return encodeHistoryCursor(lastSeen, id)
}

func decodeUserCursor(raw string) (time.Time, string, error) {
	return decodeHistoryCursor(raw)
}
