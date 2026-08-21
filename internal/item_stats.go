package internal

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

func (m *Module) itemWatchStats(ctx context.Context, itemID string, runtimeMinutes int) (watchStats, error) {
	itemID = strings.TrimSpace(itemID)
	if itemID == "" {
		return watchStats{}, fmt.Errorf("item_id required")
	}
	stats := watchStats{
		NeverWatched:        true,
		DaysSinceLastWatch:  999999,
		UserDurationMinutes: make(map[string]float64),
		UserWatchedPercent:  make(map[string]float64),
	}
	runtimeSec := float64(runtimeMinutes) * 60

	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return stats, fmt.Errorf("db not initialized")
	}

	rows, err := m.queryRows(ctx, `
		SELECT user_id, user_name, position_seconds, duration_seconds, started_at, state
		FROM sessions
		WHERE muxcore_id = ? OR item_id = ?
		ORDER BY started_at ASC`,
		itemID, itemID,
	)
	if err != nil {
		return stats, err
	}
	defer rows.Close()

	seenUsers := make(map[string]struct{})
	var lastAt time.Time

	for rows.Next() {
		var userID, userName, startedAt, state string
		var posSec, durSec float64
		if err := rows.Scan(&userID, &userName, &posSec, &durSec, &startedAt, &state); err != nil {
			return stats, err
		}
		user := normalizeWatchUser(userName)
		if user == "" {
			user = normalizeWatchUser(userID)
		}
		if user != "" {
			seenUsers[user] = struct{}{}
			durationMin := posSec / 60.0
			stats.UserDurationMinutes[user] += durationMin
			percent := watchedPercentSeconds(posSec, durSec, runtimeSec)
			if prev, ok := stats.UserWatchedPercent[user]; !ok || percent > prev {
				stats.UserWatchedPercent[user] = percent
			}
		}
		stats.NeverWatched = false
		stats.HasActivity = true
		stats.PlayCount++
		stats.ViewCount++
		stats.TotalDurationMinutes += posSec / 60.0
		if posMin := posSec / 60.0; posMin > stats.LongestDurationMinutes {
			stats.LongestDurationMinutes = posMin
		}
		if t := parseTime(startedAt); !t.IsZero() && t.After(lastAt) {
			lastAt = t
		}
		if durSec > 0 && posSec >= durSec*0.9 {
			stats.ViewCount++
		}
	}
	if err := rows.Err(); err != nil {
		return stats, err
	}

	stats.UniqueUsers = len(seenUsers)
	if !lastAt.IsZero() {
		stats.LastWatchedAt = lastAt
		stats.DaysSinceLastWatch = daysSince(lastAt)
		stats.NeverWatched = false
	}
	if stats.PlayCount == 0 && stats.ViewCount > 0 {
		stats.PlayCount = stats.ViewCount
	}
	return stats, nil
}

func (m *Module) listWatchUsers(ctx context.Context, query string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	needle := strings.ToLower(strings.TrimSpace(query))
	rows, err := m.queryRows(ctx, `
		SELECT DISTINCT COALESCE(NULLIF(user_name,''), user_id) AS username
		FROM sessions
		WHERE COALESCE(NULLIF(user_name,''), user_id) != ''
		ORDER BY username ASC
		LIMIT 1000`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := make(map[string]string)
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return nil, err
		}
		username = strings.TrimSpace(username)
		if username == "" {
			continue
		}
		key := strings.ToLower(username)
		if needle != "" && !strings.Contains(key, needle) {
			continue
		}
		if _, ok := seen[key]; !ok {
			seen[key] = username
		}
	}
	out := make([]string, 0, len(seen))
	for _, user := range seen {
		out = append(out, user)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type watchStats struct {
	ViewCount              int
	LastWatchedAt          time.Time
	NeverWatched           bool
	DaysSinceLastWatch     int
	PlayCount              int
	UniqueUsers            int
	TotalDurationMinutes   float64
	LongestDurationMinutes float64
	HasActivity            bool
	UserDurationMinutes    map[string]float64
	UserWatchedPercent     map[string]float64
}

func normalizeWatchUser(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func watchedPercentSeconds(positionSec, durationSec, runtimeSec float64) float64 {
	if durationSec > 0 && positionSec >= durationSec*0.9 {
		return 100
	}
	if runtimeSec <= 0 || positionSec <= 0 {
		return 0
	}
	pct := positionSec / runtimeSec * 100
	if pct > 100 {
		return 100
	}
	return pct
}

func daysSince(t time.Time) int {
	if t.IsZero() {
		return 999999
	}
	d := time.Since(t)
	if d < 0 {
		return 0
	}
	return int(d.Hours() / 24)
}
