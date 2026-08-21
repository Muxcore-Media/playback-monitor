package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

type streamLiveEvent struct {
	Event     string `json:"event"`
	EventType string `json:"event_type,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Active    int    `json:"active_count,omitempty"`
}

type liveHub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

func newLiveHub() *liveHub {
	return &liveHub{clients: make(map[chan []byte]struct{})}
}

func (h *liveHub) subscribe() chan []byte {
	ch := make(chan []byte, 8)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *liveHub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
	close(ch)
}

func (h *liveHub) broadcast(payload []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- payload:
		default:
		}
	}
}

func (m *Module) publishStreamLiveEvent(eventType, sessionID string) {
	active, err := m.listActiveSessions(context.Background(), "", 500)
	if err != nil {
		return
	}
	ev := streamLiveEvent{
		Event:     "session.changed",
		EventType: eventType,
		SessionID: sessionID,
		Active:    len(active),
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return
	}
	m.liveHub.broadcast(payload)
}

func (m *Module) handleStreamEventsSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := m.liveHub.subscribe()
	defer m.liveHub.unsubscribe(ch)

	fmt.Fprintf(w, "event: connected\ndata: {\"event\":\"connected\"}\n\n")
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, open := <-ch:
			if !open {
				return
			}
			fmt.Fprintf(w, "event: session\ndata: %s\n\n", msg)
			flusher.Flush()
		}
	}
}
