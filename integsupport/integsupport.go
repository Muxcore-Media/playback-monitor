// Package integsupport exposes playback-monitor internals to umbrella integration tests.
package integsupport

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Muxcore-Media/playback-monitor/internal"
)

// Module is the playback-monitor module; its gRPC handler methods
// (GetHomeStats, GetStreamAnalytics, ListHistory, Health, ...) are available directly.
type Module = internal.Module

// Config is the module configuration.
type Config = internal.Config

// SessionEvent is a playback session event accepted by IngestSession.
type SessionEvent = internal.SessionEvent

// NewTestModule builds and initialises a module backed by a temporary SQLite
// database and loopback listeners (127.0.0.1:0). Empty Config fields are
// defaulted; the module is stopped via t.Cleanup.
func NewTestModule(t *testing.T, cfg Config) *Module {
	t.Helper()
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join(t.TempDir(), "monitor.db")
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = "127.0.0.1:0"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = "127.0.0.1:0"
	}
	m := internal.NewModule(cfg)
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatalf("playback-monitor init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	return m
}

// Start starts the gRPC/HTTP servers. When MUXCORE_GRPC_ADDR is set the module
// also connects to core in the background.
func Start(ctx context.Context, m *Module) error {
	return m.Start(ctx)
}

// IngestSession ingests a session event and returns the session id and whether
// a new session row was created.
func IngestSession(ctx context.Context, m *Module, ev SessionEvent) (sessionID string, created bool, err error) {
	return m.IngestSessionForTest(ctx, ev)
}
