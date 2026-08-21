package internal

import (
	"testing"
	"time"

	playbackv1 "github.com/Muxcore-Media/playback-contract/proto/playbackv1"
)

func TestSessionEventFromPlaybackContractJSON(t *testing.T) {
	at := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	payload, err := playbackv1.MarshalSessionEvent(playbackv1.SessionInput{
		EventType:         "playback.started",
		SourceModule:      "jellyfin",
		ServerID:          "jf1",
		ServerType:        "jellyfin",
		ExternalSessionID: "sess-1",
		UserID:            "u1",
		UserName:          "alice",
		ItemID:            "item-1",
		PositionSeconds:   42,
		OccurredAt:        at,
	})
	if err != nil {
		t.Fatal(err)
	}
	ev, err := sessionEventFromPlaybackJSON("jellyfin", payload)
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventType != "playback.started" || ev.ServerID != "jf1" || ev.ExternalSessionID != "sess-1" {
		t.Fatalf("ev=%+v", ev)
	}
	if ev.ItemID != "item-1" || ev.UserName != "alice" || ev.PositionSeconds != 42 {
		t.Fatalf("fields: %+v", ev)
	}
	if !ev.OccurredAt.Equal(at) {
		t.Fatalf("occurred: %v", ev.OccurredAt)
	}
}
