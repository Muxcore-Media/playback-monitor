package internal

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type historyPageFilter struct {
	Since          time.Time
	Until          time.Time
	Watched        *bool
	RatingKey      string
	MediaItemID    string
	MediaMuxcoreID string
	ServerID       string
	MediaType      string
	ImdbID         string
	UserName       string
	UserID         string
	IdentityID     string
	TmdbID         int64
	TvdbID         int64
}

func parseHistoryPageFilter(r *http.Request) (historyPageFilter, error) {
	q := r.URL.Query()
	f := historyPageFilter{
		ServerID:  strings.TrimSpace(q.Get("server_id")),
		RatingKey: strings.TrimSpace(q.Get("rating_key")),
		MediaType: strings.TrimSpace(q.Get("media_type")),
	}
	if v := strings.TrimSpace(q.Get("item_id")); v != "" && f.RatingKey == "" {
		f.RatingKey = v
	}

	userRaw := strings.TrimSpace(q.Get("user_id"))
	if userRaw != "" {
		if _, err := uuid.Parse(userRaw); err == nil {
			f.IdentityID = userRaw
		} else {
			uid, uname, parseErr := parsePublicUserIdentityID(userRaw)
			if parseErr != nil {
				return historyPageFilter{}, parseErr
			}
			f.UserID, f.UserName = uid, uname
		}
	}

	mediaRaw := strings.TrimSpace(q.Get("media_id"))
	if mediaRaw == "" {
		mediaRaw = strings.TrimSpace(q.Get("muxcore_id"))
	}
	if mediaRaw != "" {
		if strings.Contains(mediaRaw, ":") {
			serverID, itemID, muxcoreID, err := parseMediaRef(mediaRaw)
			if err != nil {
				return historyPageFilter{}, err
			}
			f.MediaItemID = itemID
			f.MediaMuxcoreID = muxcoreID
			if f.ServerID == "" {
				f.ServerID = serverID
			}
		} else {
			f.MediaMuxcoreID = mediaRaw
		}
	}

	if sinceRaw := strings.TrimSpace(q.Get("since")); sinceRaw != "" {
		t, err := parseHistoryTime(sinceRaw)
		if err != nil {
			return historyPageFilter{}, fmt.Errorf("invalid since")
		}
		f.Since = t
	}
	if untilRaw := strings.TrimSpace(q.Get("until")); untilRaw != "" {
		t, err := parseHistoryTime(untilRaw)
		if err != nil {
			return historyPageFilter{}, fmt.Errorf("invalid until")
		}
		f.Until = t
	}
	if !f.Since.IsZero() && !f.Until.IsZero() && f.Since.After(f.Until) {
		return historyPageFilter{}, fmt.Errorf("since must be before or equal to until")
	}

	if watchedRaw := strings.TrimSpace(q.Get("watched")); watchedRaw != "" {
		watched, err := strconv.ParseBool(watchedRaw)
		if err != nil {
			return historyPageFilter{}, fmt.Errorf("invalid watched")
		}
		f.Watched = &watched
	}
	if imdb := strings.TrimSpace(q.Get("imdb_id")); imdb != "" {
		f.ImdbID = normalizeImdbID(imdb)
	}
	if raw := strings.TrimSpace(q.Get("tmdb_id")); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return historyPageFilter{}, fmt.Errorf("invalid tmdb_id")
		}
		f.TmdbID = v
	}
	if raw := strings.TrimSpace(q.Get("tvdb_id")); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return historyPageFilter{}, fmt.Errorf("invalid tvdb_id")
		}
		f.TvdbID = v
	}
	return f, nil
}

func parseHistoryTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse("2006-01-02", raw); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid time")
}

func sessionWatchedSQL() string {
	return `(position_seconds >= 60 OR (duration_seconds > 0 AND position_seconds / duration_seconds >= 0.9))`
}

func (m *Module) resolveHistoryPageFilter(ctx context.Context, f historyPageFilter) (historyPageFilter, error) {
	if f.IdentityID != "" {
		resolvedID, uid, uname, resolveErr := m.resolveIdentityID(ctx, f.IdentityID)
		if resolveErr != nil {
			return f, resolveErr
		}
		f.IdentityID = resolvedID
		if f.UserID == "" {
			f.UserID = uid
		}
		if f.UserName == "" {
			f.UserName = uname
		}
	}

	if f.MediaItemID == "" && f.MediaMuxcoreID == "" && (strings.Contains(f.RatingKey, ":") || strings.HasPrefix(f.RatingKey, "muxcore:")) {
		if rec, mediaErr := m.resolveMediaRef(ctx, f.RatingKey); mediaErr == nil && rec != nil {
			f.MediaItemID = rec.ItemID
			f.MediaMuxcoreID = rec.MuxcoreID
			if f.ServerID == "" {
				f.ServerID = rec.ServerID
			}
			f.RatingKey = ""
		}
	}
	return f, nil
}

func buildHistoryPageWhere(f historyPageFilter, cursorStarted time.Time, cursorID string) (string, []any) {
	where := []string{`state = 'stopped'`}
	args := []any{}
	if strings.TrimSpace(f.ServerID) != "" {
		where = append(where, `server_id = ?`)
		args = append(args, f.ServerID)
	}
	if strings.TrimSpace(f.IdentityID) != "" {
		where = append(where, identityMatchSQL(""))
		args = append(args, f.IdentityID)
	} else if strings.TrimSpace(f.UserID) != "" || strings.TrimSpace(f.UserName) != "" {
		where = append(where, userIdentityMatchSQL(""))
		args = append(args, f.UserID, f.UserName)
	}
	if f.MediaItemID != "" || f.MediaMuxcoreID != "" {
		where = append(where, mediaMatchWhere(""))
		args = append(args, f.MediaItemID, f.MediaMuxcoreID, f.MediaMuxcoreID)
	} else if f.RatingKey != "" {
		where = append(where, `item_id = ?`)
		args = append(args, f.RatingKey)
	}
	if f.MediaType != "" {
		where = append(where, `lower(media_type) = lower(?)`)
		args = append(args, f.MediaType)
	}
	if f.ImdbID != "" {
		where = append(where, imdbHistoryMatchSQL(""))
		args = append(args, f.ImdbID, f.ImdbID)
	}
	if f.TmdbID != 0 {
		where = append(where, tmdbHistoryMatchSQL(""))
		args = append(args, f.TmdbID, f.TmdbID)
	}
	if f.TvdbID != 0 {
		where = append(where, tvdbHistoryMatchSQL(""))
		args = append(args, f.TvdbID, f.TvdbID)
	}
	if !f.Since.IsZero() {
		where = append(where, `started_at >= ?`)
		args = append(args, f.Since.Format(time.RFC3339))
	}
	if !f.Until.IsZero() {
		where = append(where, `started_at <= ?`)
		args = append(args, f.Until.Format(time.RFC3339))
	}
	if f.Watched != nil {
		if *f.Watched {
			where = append(where, sessionWatchedSQL())
		} else {
			where = append(where, `NOT `+sessionWatchedSQL())
		}
	}
	if !cursorStarted.IsZero() {
		where = append(where, `(started_at < ? OR (started_at = ? AND id < ?))`)
		args = append(args, cursorStarted.Format(time.RFC3339Nano), cursorStarted.Format(time.RFC3339Nano), cursorID)
	}
	return strings.Join(where, " AND "), args
}

func (m *Module) listHistoryPageFiltered(ctx context.Context, f historyPageFilter, pageSize int, cursorRaw string) ([]SessionRecord, string, error) {
	if pageSize <= 0 {
		pageSize = 25
	}
	if pageSize > 100 {
		pageSize = 100
	}
	cursorStarted, cursorID, err := decodeHistoryCursor(cursorRaw)
	if err != nil {
		return nil, "", err
	}

	f, err = m.resolveHistoryPageFilter(ctx, f)
	if err != nil {
		return nil, "", err
	}

	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, "", fmt.Errorf("db not initialized")
	}

	whereSQL, args := buildHistoryPageWhere(f, cursorStarted, cursorID)
	query := `SELECT ` + sessionSelectCols + `
		FROM sessions WHERE ` + whereSQL + `
		ORDER BY started_at DESC, id DESC
		LIMIT ?`
	listArgs := append(append([]any{}, args...), pageSize+1)
	rows, err := m.querySessions(ctx, db, query, listArgs...)
	if err != nil {
		return nil, "", err
	}
	var nextCursor string
	if len(rows) > pageSize {
		last := rows[pageSize-1]
		nextCursor = encodeHistoryCursor(last.StartedAt, last.ID)
		rows = rows[:pageSize]
	}
	return rows, nextCursor, nil
}
