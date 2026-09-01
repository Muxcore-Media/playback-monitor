package internal

import (
	"net/http"
	"strings"
)

func (m *Module) operatorHTTPEnabled() bool {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return strings.TrimSpace(m.httpToken) != ""
}

func (m *Module) requireOperatorHTTPAuth(w http.ResponseWriter, r *http.Request) bool {
	if !m.operatorHTTPEnabled() {
		http.Error(w, "operator HTTP disabled; set PLAYBACK_MONITOR_HTTP_TOKEN", http.StatusServiceUnavailable)
		return false
	}
	token := operatorHTTPToken(r)
	m.cfgMu.RLock()
	expected := m.httpToken
	m.cfgMu.RUnlock()
	if token == "" || token != expected {
		http.Error(w, "missing or invalid operator token", http.StatusUnauthorized)
		return false
	}
	return true
}

func operatorHTTPToken(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-Playback-Monitor-Token")); v != "" {
		return v
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	}
	return ""
}

func (m *Module) withOperatorAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.requireOperatorHTTPAuth(w, r) {
			return
		}
		next(w, r)
	}
}
