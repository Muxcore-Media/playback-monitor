package internal

import "context"

// IngestSessionForTest ingests one session event. It is an exported hook for the
// integsupport package (umbrella integration tests) and does not alter behaviour.
func (m *Module) IngestSessionForTest(ctx context.Context, ev SessionEvent) (sessionID string, created bool, err error) {
	return m.ingestSessionEvent(ctx, ev)
}
