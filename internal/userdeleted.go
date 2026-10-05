package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	mediaevents "github.com/Muxcore-Media/contracts-media/events"
	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
	"github.com/Muxcore-Media/core/sdk/go/client"
)

// DeleteAccountPlayback removes playback history for an auth account.
// Sessions keyed by that user id are deleted. A linked identity is deleted
// only when no other server user still points at it; its remaining sessions
// go with it. Library rows stay. A repeated call deletes nothing.
func (m *Module) DeleteAccountPlayback(ctx context.Context, userID string) (int64, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return 0, fmt.Errorf("user_id is required")
	}
	rows, err := m.queryRows(ctx, `SELECT DISTINCT identity_id FROM server_users WHERE external_user_id = ?`, userID)
	if err != nil {
		return 0, fmt.Errorf("list identities: %w", err)
	}
	var identityIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, err
		}
		if id != "" {
			identityIDs = append(identityIDs, id)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	_ = rows.Close()

	var n int64
	res, err := m.exec(ctx, `DELETE FROM sessions WHERE user_id = ?`, userID)
	if err != nil {
		return 0, fmt.Errorf("delete sessions: %w", err)
	}
	if affected, err := res.RowsAffected(); err == nil {
		n += affected
	}
	if _, err := m.exec(ctx, `DELETE FROM server_users WHERE external_user_id = ?`, userID); err != nil {
		return n, fmt.Errorf("delete server users: %w", err)
	}
	for _, id := range identityIDs {
		var left int
		if err := m.queryRow(ctx, `SELECT COUNT(1) FROM server_users WHERE identity_id = ?`, id).Scan(&left); err != nil {
			return n, err
		}
		if left != 0 {
			continue
		}
		res, err := m.exec(ctx, `DELETE FROM sessions WHERE identity_id = ?`, id)
		if err != nil {
			return n, fmt.Errorf("delete identity sessions: %w", err)
		}
		if affected, err := res.RowsAffected(); err == nil {
			n += affected
		}
		if _, err := m.exec(ctx, `DELETE FROM user_identities WHERE id = ?`, id); err != nil {
			return n, fmt.Errorf("delete identity: %w", err)
		}
	}
	return n, nil
}

// ApplyUserDeleted deletes playback history for the account in an
// identity.user.deleted payload.
func (m *Module) ApplyUserDeleted(ctx context.Context, payload []byte) error {
	var body mediaevents.UserDeletedPayload
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode identity.user.deleted: %w", err)
	}
	if strings.TrimSpace(body.UserID) == "" {
		return fmt.Errorf("identity.user.deleted missing user_id")
	}
	_, err := m.DeleteAccountPlayback(ctx, body.UserID)
	return err
}

func (m *Module) subscribeUserDeleted(ctx context.Context, mc *client.Client, wg *sync.WaitGroup, active *int) {
	ch, cancel, err := mc.Events.Subscribe(ctx, mediaevents.EventIdentityUserDeleted)
	if err != nil {
		slog.Debug("playback-monitor: subscribe identity.user.deleted failed", "error", err)
		return
	}
	*active++
	wg.Add(1)
	go func(events <-chan *eventsv1.Event, cancel context.CancelFunc) {
		defer wg.Done()
		defer cancel()
		for evt := range events {
			if evt == nil {
				continue
			}
			if err := m.ApplyUserDeleted(ctx, evt.Payload); err != nil {
				slog.Warn("playback-monitor: apply identity.user.deleted", "error", err)
			}
		}
	}(ch, cancel)
}
