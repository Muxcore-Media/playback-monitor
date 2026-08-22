package internal

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
	notificationv1 "github.com/Muxcore-Media/contracts-notification/muxcore/notification/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (m *Module) findCapabilityAddr(ctx context.Context, capability string) (string, error) {
	m.mu.RLock()
	mc := m.mc
	m.mu.RUnlock()
	if mc == nil {
		return "", fmt.Errorf("not connected to core")
	}
	modules, err := mc.Discovery.FindByCapability(ctx, capability)
	if err != nil {
		return "", err
	}
	for _, mod := range modules {
		addr := dialAddrForModule(mod.Id, mod.HttpAddr)
		if addr != "" {
			return addr, nil
		}
	}
	return "", fmt.Errorf("no %s module found", capability)
}

func dialAddrForModule(moduleID, httpAddr string) string {
	if httpAddr == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(httpAddr)
	if err != nil || port == "" {
		return httpAddr
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return net.JoinHostPort(host, port)
	}
	if os.Getenv("MUXCORE_MESH_DIAL_LOCAL") == "true" {
		return net.JoinHostPort("127.0.0.1", port)
	}
	if moduleID != "" {
		return net.JoinHostPort(moduleID, port)
	}
	return httpAddr
}

func (m *Module) notifySessionStartLegacy(ctx context.Context, ev SessionEvent) {
	if !m.getNotifyOnSessionStart() {
		return
	}
	user := firstNonEmptyStr(ev.UserName, ev.UserID, "unknown")
	title := ev.Title
	if title == "" {
		title = ev.ItemID
	}
	msg := fmt.Sprintf("%s started watching %q", user, title)
	m.postNotification(ctx, "Playback started", msg, "info", map[string]string{
		"event":      playbackevents.EventPlaybackStarted,
		"user":       user,
		"title":      title,
		"session_id": ev.ExternalSessionID,
		"platform":   ev.Platform,
	})
}

func (m *Module) notifySessionStopLegacy(ctx context.Context, ev SessionEvent) {
	if !m.getNotifyOnSessionStop() {
		return
	}
	user := firstNonEmptyStr(ev.UserName, ev.UserID, "unknown")
	title := ev.Title
	if title == "" {
		title = ev.ItemID
	}
	msg := fmt.Sprintf("%s finished watching %q (%s watched)", user, title, formatMinutes(ev.PositionSeconds))
	m.postNotification(ctx, "Playback stopped", msg, "info", map[string]string{
		"event":            playbackevents.EventPlaybackStopped,
		"user":             user,
		"title":            title,
		"session_id":       ev.ExternalSessionID,
		"position_seconds": fmt.Sprintf("%d", ev.PositionSeconds),
	})
}

func (m *Module) notifySessionStart(ctx context.Context, ev SessionEvent) {
	m.firePlaybackNotificationRules(ctx, playbackevents.EventPlaybackStarted, ev)
}

func (m *Module) notifySessionStop(ctx context.Context, ev SessionEvent) {
	m.firePlaybackNotificationRules(ctx, playbackevents.EventPlaybackStopped, ev)
}

func (m *Module) postNotification(ctx context.Context, title, message, severity string, fields map[string]string) {
	addr, err := m.findCapabilityAddr(ctx, "notification")
	if err != nil {
		return
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()
	cli := notificationv1.NewNotificationServiceClient(conn)
	_, _ = cli.Notify(ctx, &notificationv1.NotifyRequest{
		Title:        title,
		Message:      message,
		Severity:     severity,
		SourceModule: m.id,
		Fields:       fields,
	})
}

func (m *Module) getNotifyOnSessionStart() bool {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return m.notifyOnSessionStart
}

func (m *Module) getNotifyOnSessionStop() bool {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return m.notifyOnSessionStop
}

func firstNonEmptyStr(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func formatMinutes(sec int64) string {
	if sec <= 0 {
		return "0m"
	}
	m := sec / 60
	if m >= 60 {
		return fmt.Sprintf("%dh %dm", m/60, m%60)
	}
	return fmt.Sprintf("%dm", m)
}

func envTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
