package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type libraryStorageHistoryPoint struct {
	Day        string
	TotalBytes int64
	ItemCount  int
}

type libraryStoragePrediction struct {
	GrowthBytesPerDay float64
	ProjectedBytes    int64
	HorizonDays       int
}

func todayUTC() string {
	return time.Now().UTC().Format("2006-01-02")
}

func (m *Module) snapshotLibraryStorageToday(ctx context.Context, serverID string) error {
	summary, err := m.getLibraryStorageSummary(ctx, serverID)
	if err != nil {
		return err
	}
	if summary.TotalItems == 0 && summary.TotalBytes == 0 {
		return nil
	}
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	day := todayUTC()
	serverKey := strings.TrimSpace(serverID)
	_, err = m.exec(ctx, `
		INSERT INTO library_storage_daily(day, server_id, total_bytes, item_count)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(day, server_id) DO UPDATE SET
			total_bytes = excluded.total_bytes,
			item_count = excluded.item_count`,
		day, serverKey, summary.TotalBytes, summary.TotalItems,
	)
	if err != nil {
		return err
	}
	for _, lib := range summary.Libraries {
		_, err = m.exec(ctx, `
			INSERT INTO library_storage_by_library_daily(day, server_id, library_name, total_bytes, item_count)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(day, server_id, library_name) DO UPDATE SET
				total_bytes = excluded.total_bytes,
				item_count = excluded.item_count`,
			day, lib.ServerID, lib.LibraryName, lib.TotalBytes, lib.ItemCount,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) getLibraryStorageHistory(ctx context.Context, serverID, libraryName string, days int) ([]libraryStorageHistoryPoint, error) {
	if days <= 0 {
		days = 90
	}
	if days > 365 {
		days = 365
	}
	if err := m.snapshotLibraryStorageToday(ctx, serverID); err != nil {
		return nil, err
	}
	since := time.Now().UTC().AddDate(0, 0, -days).Format("2006-01-02")
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	libraryName = strings.TrimSpace(libraryName)
	if libraryName != "" {
		query := `
			SELECT day, total_bytes, item_count
			FROM library_storage_by_library_daily
			WHERE day >= ? AND library_name = ?`
		args := []any{since, libraryName}
		if strings.TrimSpace(serverID) != "" {
			query += ` AND server_id = ?`
			args = append(args, serverID)
		}
		query += ` ORDER BY day ASC`
		return m.queryLibraryStorageHistoryRows(ctx, db, query, args...)
	}

	query := `
		SELECT day, total_bytes, item_count
		FROM library_storage_daily
		WHERE day >= ?`
	args := []any{since}
	if strings.TrimSpace(serverID) != "" {
		query += ` AND server_id = ?`
		args = append(args, serverID)
	} else {
		query += ` AND server_id = ''`
	}
	query += ` ORDER BY day ASC`
	return m.queryLibraryStorageHistoryRows(ctx, db, query, args...)
}

func (m *Module) queryLibraryStorageHistoryRows(ctx context.Context, db *sql.DB, query string, args ...any) ([]libraryStorageHistoryPoint, error) {
	rows, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]libraryStorageHistoryPoint, 0)
	for rows.Next() {
		var p libraryStorageHistoryPoint
		if err := rows.Scan(&p.Day, &p.TotalBytes, &p.ItemCount); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func predictLibraryStorageGrowth(points []libraryStorageHistoryPoint, horizonDays int) libraryStoragePrediction {
	if horizonDays <= 0 {
		horizonDays = 90
	}
	if len(points) < 2 {
		lastBytes := int64(0)
		if len(points) == 1 {
			lastBytes = points[0].TotalBytes
		}
		return libraryStoragePrediction{ProjectedBytes: lastBytes, HorizonDays: horizonDays}
	}
	first := points[0]
	last := points[len(points)-1]
	start, err1 := time.Parse("2006-01-02", first.Day)
	end, err2 := time.Parse("2006-01-02", last.Day)
	if err1 != nil || err2 != nil {
		return libraryStoragePrediction{}
	}
	spanDays := end.Sub(start).Hours() / 24
	if spanDays < 1 {
		spanDays = 1
	}
	growth := float64(last.TotalBytes-first.TotalBytes) / spanDays
	projected := last.TotalBytes + int64(growth*float64(horizonDays))
	if projected < 0 {
		projected = 0
	}
	return libraryStoragePrediction{
		GrowthBytesPerDay: growth,
		ProjectedBytes:    projected,
		HorizonDays:       horizonDays,
	}
}
