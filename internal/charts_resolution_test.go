package internal

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func TestPlaysByStreamResolution(t *testing.T) {
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

	for i, res := range []string{"1080", "1080", "720", "4k"} {
		ev := SessionEvent{
			EventType: "playback.started", ServerID: "s1",
			ExternalSessionID: fmt.Sprintf("res-%d", i),
			ItemID:            fmt.Sprintf("item-%d", i), StreamResolution: res,
		}
		if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
		stop := ev
		stop.EventType = "playback.stopped"
		if _, _, err := m.ingestSessionEvent(ctx, stop); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := m.playsByStreamResolution(ctx, 30, 10)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, row := range rows {
		counts[row.Key] = row.Count
	}
	if counts["1080"] != 2 || counts["720"] != 1 || counts["4k"] != 1 {
		t.Fatalf("counts=%v rows=%+v", counts, rows)
	}
}
