package internal

import (
	"context"
	"encoding/json"
	"log/slog"

	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
)

func (m *Module) subscribeGuardViolationEvents() {
	mc := m.eventClient()
	if mc == nil {
		return
	}
	ch, cancel, err := mc.Events.Subscribe(context.Background(), "playback.guard.violation")
	if err != nil {
		slog.Debug("playback-monitor: subscribe guard violations failed", "error", err)
		return
	}
	go func(events <-chan *eventsv1.Event) {
		defer cancel()
		for evt := range events {
			m.handleGuardViolationEvent(evt)
		}
	}(ch)
}

func (m *Module) handleGuardViolationEvent(evt *eventsv1.Event) {
	if evt == nil || len(evt.Payload) == 0 {
		return
	}
	var data guardViolationPayload
	if err := json.Unmarshal(evt.Payload, &data); err != nil {
		slog.Debug("playback-monitor: bad guard violation payload", "error", err)
		return
	}
	m.fireGuardViolationNotificationRules(context.Background(), data)
}
