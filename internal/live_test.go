package internal

import (
	"encoding/json"
	"testing"
)

func TestLiveHubBroadcast(t *testing.T) {
	h := newLiveHub()
	ch := h.subscribe()
	payload, _ := json.Marshal(streamLiveEvent{Event: "session.changed", Active: 2})
	h.broadcast(payload)

	select {
	case got := <-ch:
		if string(got) != string(payload) {
			t.Fatalf("payload mismatch: %s", got)
		}
	default:
		t.Fatal("expected broadcast message")
	}
	h.unsubscribe(ch)
}
