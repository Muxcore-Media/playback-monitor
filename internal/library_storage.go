package internal

import (
	"context"
	"fmt"
	"strings"
)

type libraryStorageRow struct {
	ServerID    string
	LibraryName string
	ItemCount   int
	TotalBytes  int64
}

type libraryStorageSummary struct {
	TotalItems       int
	TotalBytes       int64
	DuplicateWaste   int64
	Libraries        []libraryStorageRow
}

func (m *Module) getLibraryStorageSummary(ctx context.Context, serverID string) (libraryStorageSummary, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return libraryStorageSummary{}, fmt.Errorf("db not initialized")
	}

	query := `
		SELECT server_id,
			COALESCE(NULLIF(library_name, ''), '(unknown)') AS library_name,
			COUNT(1),
			COALESCE(SUM(file_size_bytes), 0)
		FROM library_items
		WHERE ` + activeLibraryItemsSQL("")
	args := []any{}
	if strings.TrimSpace(serverID) != "" {
		query += ` AND server_id = ?`
		args = append(args, serverID)
	}
	query += ` GROUP BY server_id, library_name ORDER BY SUM(file_size_bytes) DESC`

	rows, err := m.queryRows(ctx, query, args...)
	if err != nil {
		return libraryStorageSummary{}, err
	}
	defer rows.Close()

	summary := libraryStorageSummary{Libraries: make([]libraryStorageRow, 0)}
	for rows.Next() {
		var row libraryStorageRow
		if err := rows.Scan(&row.ServerID, &row.LibraryName, &row.ItemCount, &row.TotalBytes); err != nil {
			return libraryStorageSummary{}, err
		}
		summary.TotalItems += row.ItemCount
		summary.TotalBytes += row.TotalBytes
		summary.Libraries = append(summary.Libraries, row)
	}
	if err := rows.Err(); err != nil {
		return libraryStorageSummary{}, err
	}

	groups, err := m.listLibraryDuplicates(ctx, serverID, 100)
	if err == nil {
		for _, g := range groups {
			if len(g.Copies) < 2 {
				continue
			}
			var size int64
			for _, c := range g.Copies {
				if c.FileSizeBytes > size {
					size = c.FileSizeBytes
				}
			}
			if size > 0 {
				summary.DuplicateWaste += size * int64(len(g.Copies)-1)
			}
		}
	}
	return summary, nil
}

func formatBytes(n int64) string {
	if n <= 0 {
		return "0 B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for cur := n / unit; cur >= unit; cur /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}
