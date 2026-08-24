package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type playsByDateChartOpts struct {
	YAxis    string
	UserIDs  []string
	Days     int
	Grouping bool
}

type playsByDateSeries struct {
	Name string
	Data []int
}

type playsByDateChart struct {
	Categories []string
	Series     []playsByDateSeries
}

func parseChartUserIDs(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, err := uuid.Parse(part); err != nil {
			return nil, fmt.Errorf("invalid user_id")
		}
		out = append(out, part)
	}
	return out, nil
}

func mediaBucket(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "episode", "show", "series":
		return "tv"
	case "movie":
		return "movie"
	case "track", "audio", "music":
		return "music"
	case "livetv", "live":
		return "live"
	default:
		return "other"
	}
}

func (m *Module) playsByDateChart(ctx context.Context, opts playsByDateChartOpts) (playsByDateChart, error) {
	if opts.Days <= 0 {
		opts.Days = 30
	}
	yAxis := strings.ToLower(strings.TrimSpace(opts.YAxis))
	if yAxis == "" {
		yAxis = "plays"
	}
	if yAxis != "plays" && yAxis != "duration" {
		return playsByDateChart{}, fmt.Errorf("invalid y_axis")
	}

	since, days := m.chartSince(opts.Days)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return playsByDateChart{}, fmt.Errorf("db not initialized")
	}

	metricExpr := "1"
	if yAxis == "duration" {
		metricExpr = "CAST(COALESCE(NULLIF(position_seconds, 0), duration_seconds, 0) AS INTEGER)"
	}

	where := `state = 'stopped' AND started_at >= ?`
	args := []any{since}
	if len(opts.UserIDs) > 0 {
		where += ` AND identity_id IN (` + strings.TrimRight(strings.Repeat("?,", len(opts.UserIDs)), ",") + `)`
		for _, id := range opts.UserIDs {
			args = append(args, id)
		}
	}

	groupKey := "id"
	if opts.Grouping {
		groupKey = `COALESCE(NULLIF(external_session_id, ''), id)`
	}

	query := fmt.Sprintf(`
		SELECT day, media_type, SUM(metric) AS val
		FROM (
			SELECT substr(started_at, 1, 10) AS day,
				lower(media_type) AS media_type,
				SUM(%s) AS metric
			FROM sessions
			WHERE %s
			GROUP BY day, %s, lower(media_type)
		) grouped
		GROUP BY day, media_type
		ORDER BY day ASC`, metricExpr, where, groupKey)

	rows, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return playsByDateChart{}, err
	}
	defer func() { _ = rows.Close() }()

	type dayBucket struct {
		tv, movie, music, live int
	}
	byDay := map[string]dayBucket{}
	for rows.Next() {
		var day, mediaType string
		var val int
		if err := rows.Scan(&day, &mediaType, &val); err != nil {
			return playsByDateChart{}, err
		}
		b := byDay[day]
		switch mediaBucket(mediaType) {
		case "tv":
			b.tv += val
		case "movie":
			b.movie += val
		case "music":
			b.music += val
		case "live":
			b.live += val
		}
		byDay[day] = b
	}
	if err := rows.Err(); err != nil {
		return playsByDateChart{}, err
	}

	categories := make([]string, 0, days)
	tvData := make([]int, 0, days)
	movieData := make([]int, 0, days)
	musicData := make([]int, 0, days)
	liveData := make([]int, 0, days)
	totalData := make([]int, 0, days)

	base := time.Now().UTC()
	for i := days - 1; i >= 0; i-- {
		day := base.AddDate(0, 0, -i).Format("2006-01-02")
		b := byDay[day]
		categories = append(categories, day)
		tvData = append(tvData, b.tv)
		movieData = append(movieData, b.movie)
		musicData = append(musicData, b.music)
		liveData = append(liveData, b.live)
		totalData = append(totalData, b.tv+b.movie+b.music+b.live)
	}

	series := make([]playsByDateSeries, 0, 5)
	if hasNonZero(tvData) {
		series = append(series, playsByDateSeries{Name: "TV", Data: tvData})
	}
	if hasNonZero(movieData) {
		series = append(series, playsByDateSeries{Name: "Movies", Data: movieData})
	}
	if hasNonZero(musicData) {
		series = append(series, playsByDateSeries{Name: "Music", Data: musicData})
	}
	if hasNonZero(liveData) {
		series = append(series, playsByDateSeries{Name: "Live TV", Data: liveData})
	}
	if len(series) > 0 {
		series = append(series, playsByDateSeries{Name: "Total", Data: totalData})
	}

	return playsByDateChart{Categories: categories, Series: series}, nil
}

func hasNonZero(vals []int) bool {
	for _, v := range vals {
		if v > 0 {
			return true
		}
	}
	return false
}

func playsByDateChartToJSON(chart playsByDateChart) map[string]any {
	series := make([]map[string]any, 0, len(chart.Series))
	for _, s := range chart.Series {
		series = append(series, map[string]any{
			"name": s.Name,
			"data": s.Data,
		})
	}
	return map[string]any{
		"categories": chart.Categories,
		"series":     series,
	}
}
