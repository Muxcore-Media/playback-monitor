package internal

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type concurrentInterval struct {
	start time.Time
	stop  time.Time
}

type concurrentSeries struct {
	Name string `json:"name"`
	Data []int  `json:"data"`
}

type concurrentStreamsChart struct {
	Categories []string           `json:"categories"`
	Series     []concurrentSeries `json:"series"`
}

func peakConcurrent(intervals []concurrentInterval) int {
	if len(intervals) == 0 {
		return 0
	}
	type event struct {
		at    time.Time
		delta int
	}
	events := make([]event, 0, len(intervals)*2)
	for _, iv := range intervals {
		if iv.stop.Before(iv.start) {
			continue
		}
		events = append(events, event{at: iv.start, delta: 1}, event{at: iv.stop, delta: -1})
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].at.Equal(events[j].at) {
			return events[i].delta > events[j].delta
		}
		return events[i].at.Before(events[j].at)
	})
	cur, peak := 0, 0
	for _, ev := range events {
		cur += ev.delta
		if cur > peak {
			peak = cur
		}
	}
	return peak
}

func parseSessionInterval(startedAt, stoppedAt string) (concurrentInterval, bool) {
	startedAt = strings.TrimSpace(startedAt)
	if startedAt == "" {
		return concurrentInterval{}, false
	}
	start, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return concurrentInterval{}, false
	}
	stop := start
	if s := strings.TrimSpace(stoppedAt); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			stop = t
		}
	}
	if stop.Before(start) {
		stop = start
	}
	return concurrentInterval{start: start, stop: stop}, true
}

type concurrentStreamDayRow struct {
	Date      string
	Total     int
	Direct    int
	Transcode int
}

func concurrentChartToRows(chart concurrentStreamsChart) []concurrentStreamDayRow {
	if len(chart.Categories) == 0 {
		return nil
	}
	seriesByName := make(map[string][]int, len(chart.Series))
	for _, s := range chart.Series {
		seriesByName[s.Name] = s.Data
	}
	total := seriesByName["Total"]
	direct := seriesByName["Direct"]
	transcode := seriesByName["Transcode"]
	rows := make([]concurrentStreamDayRow, 0, len(chart.Categories))
	for i, date := range chart.Categories {
		row := concurrentStreamDayRow{Date: date}
		if i < len(total) {
			row.Total = total[i]
		}
		if i < len(direct) {
			row.Direct = direct[i]
		}
		if i < len(transcode) {
			row.Transcode = transcode[i]
		}
		rows = append(rows, row)
	}
	return rows
}

func (m *Module) concurrentStreamRows(ctx context.Context, days int) ([]concurrentStreamDayRow, error) {
	chart, err := m.concurrentStreamsByStreamType(ctx, days)
	if err != nil {
		return nil, err
	}
	return concurrentChartToRows(chart), nil
}

func (m *Module) concurrentStreamsByStreamType(ctx context.Context, days int) (concurrentStreamsChart, error) {
	since, dayCount := m.chartSince(days)
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return concurrentStreamsChart{}, fmt.Errorf("db not initialized")
	}
	rows, err := m.queryRows(ctx, `
		SELECT started_at, stopped_at, is_transcode
		FROM sessions
		WHERE state = 'stopped' AND started_at >= ? AND stopped_at != ''`, since)
	if err != nil {
		return concurrentStreamsChart{}, err
	}
	defer func() { _ = rows.Close() }()

	byDayDirect := make(map[string][]concurrentInterval)
	byDayTranscode := make(map[string][]concurrentInterval)
	for rows.Next() {
		var startedAt, stoppedAt string
		var isTranscode int
		if err := rows.Scan(&startedAt, &stoppedAt, &isTranscode); err != nil {
			return concurrentStreamsChart{}, err
		}
		iv, ok := parseSessionInterval(startedAt, stoppedAt)
		if !ok {
			continue
		}
		day := iv.start.UTC().Format("2006-01-02")
		if isTranscode != 0 {
			byDayTranscode[day] = append(byDayTranscode[day], iv)
		} else {
			byDayDirect[day] = append(byDayDirect[day], iv)
		}
	}
	if err := rows.Err(); err != nil {
		return concurrentStreamsChart{}, err
	}

	base := time.Now().UTC().Truncate(24 * time.Hour)
	categories := make([]string, 0, dayCount)
	directData := make([]int, 0, dayCount)
	transcodeData := make([]int, 0, dayCount)
	totalData := make([]int, 0, dayCount)
	for i := dayCount - 1; i >= 0; i-- {
		day := base.AddDate(0, 0, -i).Format("2006-01-02")
		categories = append(categories, day)
		direct := peakConcurrent(byDayDirect[day])
		transcode := peakConcurrent(byDayTranscode[day])
		directData = append(directData, direct)
		transcodeData = append(transcodeData, transcode)
		totalData = append(totalData, peakConcurrent(append(byDayDirect[day], byDayTranscode[day]...)))
	}
	return concurrentStreamsChart{
		Categories: categories,
		Series: []concurrentSeries{
			{Name: "Total", Data: totalData},
			{Name: "Direct", Data: directData},
			{Name: "Transcode", Data: transcodeData},
		},
	}, nil
}
