package internal

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func TestPlaysByTopUsersAndStreamType(t *testing.T) {
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

	for i, user := range []string{"alice", "bob", "alice"} {
		ev := SessionEvent{
			EventType:         "playback.started",
			ExternalSessionID: fmt.Sprintf("tu-%d", i),
			UserName:          user,
			ItemID:            "item-1",
			IsTranscode:       i == 1,
		}
		if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
		ev.EventType = "playback.stopped"
		ev.IsTranscode = i == 1
		ev.Platform = "Android"
		if _, _, err := m.ingestSessionEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}

	users, err := m.playsByTopUsers(ctx, 30, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 || users[0].Label != "alice" || users[0].Count != 2 {
		t.Fatalf("top users: %+v", users)
	}
	streamTypes, err := m.playsByStreamType(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(streamTypes) == 0 {
		t.Fatalf("stream types: %+v", streamTypes)
	}
}
