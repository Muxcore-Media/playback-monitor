package internal

import (
	"context"
	"encoding/json"
	"log/slog"

	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
)

func (m *Module) handleGuardViolationEvent(ctx context.Context, evt *eventsv1.Event) {
	if evt == nil || len(evt.Payload) == 0 {
		return
	}
	var data guardViolationPayload
	if err := json.Unmarshal(evt.Payload, &data); err != nil {
		slog.Debug("playback-monitor: bad guard violation payload", "error", err)
		return
	}
	m.fireGuardViolationNotificationRules(ctx, data)
}
