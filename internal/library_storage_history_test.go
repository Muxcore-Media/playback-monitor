package internal

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestLibraryStorageHistoryAndPrediction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := NewModule(Config{
		DBPath:   filepath.Join(dir, "monitor.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	day1 := time.Now().UTC().AddDate(0, 0, -10).Format("2006-01-02")
	day2 := time.Now().UTC().AddDate(0, 0, -5).Format("2006-01-02")
	for _, row := range []struct {
		day         string
		bytes       int64
		items       int
	}{
		{day1, 1_000_000_000, 100},
		{day2, 2_000_000_000, 120},
	} {
		if _, err := m.exec(ctx, `
			INSERT INTO library_storage_daily(day, server_id, total_bytes, item_count)
			VALUES (?, '', ?, ?)`, row.day, row.bytes, row.items); err != nil {
			t.Fatal(err)
		}
	}

	points, err := m.getLibraryStorageHistory(ctx, "", "", 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) < 2 {
		t.Fatalf("history: %+v", points)
	}
	pred := predictLibraryStorageGrowth(points, 90)
	if pred.GrowthBytesPerDay <= 0 {
		t.Fatalf("growth: %+v", pred)
	}
	if pred.ProjectedBytes <= points[len(points)-1].TotalBytes {
		t.Fatalf("projected: %+v last=%d", pred, points[len(points)-1].TotalBytes)
	}
}
