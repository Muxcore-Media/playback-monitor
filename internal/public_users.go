package internal

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type publicUserAccount struct {
	ServerID       string
	ServerType     string
	ExternalUserID string
	Username       string
}

type publicUserIdentity struct {
	ID       string
	UserID   string
	UserName string
	Accounts []publicUserAccount
	LastSeen time.Time
}

func publicUserIdentityID(userID, userName string) string {
	userID = strings.TrimSpace(userID)
	if userID != "" {
		return userID
	}
	return "name:" + strings.ToLower(strings.TrimSpace(userName))
}

func (m *Module) listPublicUserIdentities(ctx context.Context, pageSize int, cursorRaw string) ([]publicUserIdentity, string, error) {
	if pageSize <= 0 {
		pageSize = 25
	}
	if pageSize > 100 {
		pageSize = 100
	}
	cursorSeen, cursorID, err := decodeUserCursor(cursorRaw)
	if err != nil {
		return nil, "", err
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, "", fmt.Errorf("db not initialized")
	}

	where := []string{`1=1`}
	args := []any{}
	if !cursorSeen.IsZero() {
		where = append(where, `(last_seen < ? OR (last_seen = ? AND identity_id < ?))`)
		ts := cursorSeen.Format(time.RFC3339Nano)
		args = append(args, ts, ts, cursorID)
	}
	whereSQL := strings.Join(where, " AND ")
	query := `
		WITH identities AS (
			SELECT
				ui.id AS identity_id,
				ui.display_name AS user_name,
				MAX(su.last_seen_at) AS last_seen
			FROM user_identities ui
			JOIN server_users su ON su.identity_id = ui.id
			GROUP BY ui.id
		)
		SELECT identity_id, user_name, last_seen
		FROM identities
		WHERE ` + whereSQL + `
		ORDER BY last_seen DESC, identity_id DESC
		LIMIT ?`
	listArgs := append(append([]any{}, args...), pageSize+1)
	rows, err := m.queryRows(ctx, query, listArgs...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	idents := make([]publicUserIdentity, 0)
	for rows.Next() {
		var id, userName, lastSeenStr string
		if err := rows.Scan(&id, &userName, &lastSeenStr); err != nil {
			return nil, "", err
		}
		lastSeen, _ := time.Parse(time.RFC3339Nano, lastSeenStr)
		if lastSeen.IsZero() {
			lastSeen, _ = time.Parse(time.RFC3339, lastSeenStr)
		}
		idents = append(idents, publicUserIdentity{
			ID: id, UserName: userName, LastSeen: lastSeen,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(idents) > pageSize {
		last := idents[pageSize-1]
		nextCursor = encodeUserCursor(last.LastSeen, last.ID)
		idents = idents[:pageSize]
	}

	for i := range idents {
		accounts, err := m.listServerUsersForIdentity(ctx, idents[i].ID)
		if err != nil {
			return nil, "", err
		}
		idents[i].Accounts = accounts
	}
	return idents, nextCursor, nil
}

func (m *Module) listPublicUserAccounts(ctx context.Context, userID, userName string) ([]publicUserAccount, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT DISTINCT server_id, server_type, user_id, user_name
		FROM sessions
		WHERE `+userIdentityMatchSQL("")+`
		ORDER BY server_id ASC`, userID, userName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (m *Module) getPublicUserIdentity(ctx context.Context, identityID string) (*publicUserIdentity, error) {
	resolvedID, _, userName, err := m.resolveIdentityID(ctx, identityID)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	var displayName, lastSeenStr string
	err = m.queryRow(ctx, `
		SELECT ui.display_name, MAX(su.last_seen_at)
		FROM user_identities ui
		JOIN server_users su ON su.identity_id = ui.id
		WHERE ui.id = ?
		GROUP BY ui.id`, resolvedID,
	).Scan(&displayName, &lastSeenStr)
	if err != nil {
		return nil, err
	}
	if userName == "" {
		userName = displayName
	}
	lastSeen, _ := time.Parse(time.RFC3339Nano, lastSeenStr)
	if lastSeen.IsZero() {
		lastSeen, _ = time.Parse(time.RFC3339, lastSeenStr)
	}
	accounts, err := m.listServerUsersForIdentity(ctx, resolvedID)
	if err != nil {
		return nil, err
	}
	return &publicUserIdentity{
		ID: resolvedID, UserName: userName, LastSeen: lastSeen, Accounts: accounts,
	}, nil
}

type userWindowStats struct {
	Plays        int
	WatchMinutes float64
}

func (m *Module) publicUserStats(ctx context.Context, identityID string) (map[string]userWindowStats, error) {
	windows := map[string]int{
		"last_7":  7,
		"last_30": 30,
	}
	out := map[string]userWindowStats{
		"all_time": {},
		"last_7":   {},
		"last_30":  {},
	}
	resolvedID, _, _, err := m.resolveIdentityID(ctx, identityID)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	match := identityMatchSQL("")
	for key, days := range windows {
		since := time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339)
		var plays int
		var minutes float64
		err := m.queryRow(ctx, `
			SELECT COUNT(1), COALESCE(SUM(position_seconds), 0) / 60.0
			FROM sessions
			WHERE state = 'stopped' AND started_at >= ? AND `+match,
			append([]any{since}, resolvedID)...,
		).Scan(&plays, &minutes)
		if err != nil {
			return nil, err
		}
		out[key] = userWindowStats{Plays: plays, WatchMinutes: minutes}
	}
	var plays int
	var minutes float64
	err = m.queryRow(ctx, `
		SELECT COUNT(1), COALESCE(SUM(position_seconds), 0) / 60.0
		FROM sessions
		WHERE state = 'stopped' AND `+match, resolvedID,
	).Scan(&plays, &minutes)
	if err != nil {
		return nil, err
	}
	out["all_time"] = userWindowStats{Plays: plays, WatchMinutes: minutes}
	return out, nil
}

func publicUserIdentityJSON(id publicUserIdentity) map[string]any {
	accounts := make([]map[string]any, 0, len(id.Accounts))
	for _, acc := range id.Accounts {
		accounts = append(accounts, map[string]any{
			"server_id":        acc.ServerID,
			"server_type":      acc.ServerType,
			"external_user_id": acc.ExternalUserID,
			"username":         acc.Username,
		})
	}
	return map[string]any{
		"id":       id.ID,
		"username": id.UserName,
		"accounts": accounts,
	}
}

func publicUserStatsJSON(identityID, userName string, stats map[string]userWindowStats) map[string]any {
	windows := make(map[string]any, len(stats))
	for key, stat := range stats {
		windows[key] = map[string]any{
			"plays":         stat.Plays,
			"watch_minutes": stat.WatchMinutes,
		}
	}
	return map[string]any{
		"id":       identityID,
		"username": userName,
		"windows":  windows,
	}
}

func (m *Module) handlePublicUsers(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize == 0 {
		pageSize, _ = strconv.Atoi(r.URL.Query().Get("page_size"))
	}
	idents, nextCursor, err := m.listPublicUserIdentities(r.Context(), pageSize, r.URL.Query().Get("cursor"))
	if err != nil {
		if strings.Contains(err.Error(), "invalid cursor") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if pageSize <= 0 {
		pageSize = 25
	}
	data := make([]map[string]any, 0, len(idents))
	for _, id := range idents {
		data = append(data, publicUserIdentityJSON(id))
	}
	meta := map[string]any{"nextCursor": nil, "pageSize": pageSize}
	if nextCursor != "" {
		meta["nextCursor"] = nextCursor
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": meta})
}

func (m *Module) handlePublicUserByID(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	ident, err := m.getPublicUserIdentity(r.Context(), id)
	if err != nil {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, publicUserIdentityJSON(*ident))
}

func (m *Module) handlePublicUserStats(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	ident, err := m.getPublicUserIdentity(r.Context(), id)
	if err != nil {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	stats, err := m.publicUserStats(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, publicUserStatsJSON(id, ident.UserName, stats))
}

func (m *Module) handlePublicUserHistory(w http.ResponseWriter, r *http.Request) {
	if !m.requirePublicAPIAuth(w, r) {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	ident, err := m.getPublicUserIdentity(r.Context(), id)
	if err != nil {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize == 0 {
		pageSize, _ = strconv.Atoi(r.URL.Query().Get("page_size"))
	}
	rows, nextCursor, err := m.listHistoryPageByIdentity(
		r.Context(),
		r.URL.Query().Get("server_id"),
		ident.ID,
		pageSize,
		r.URL.Query().Get("cursor"),
	)
	if err != nil {
		if strings.Contains(err.Error(), "invalid cursor") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if pageSize <= 0 {
		pageSize = 25
	}
	data := make([]map[string]any, 0, len(rows))
	for _, rec := range rows {
		data = append(data, publicHistoryFromRecord(rec))
	}
	meta := map[string]any{"nextCursor": nil, "pageSize": pageSize}
	if nextCursor != "" {
		meta["nextCursor"] = nextCursor
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": data, "meta": meta})
}
