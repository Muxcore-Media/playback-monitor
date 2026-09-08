package internal

import (
	"net"
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

// withIngestAuth allows household BFF ingest from loopback without a token
// (run-host mediauiprox → :8560). Non-loopback clients still need the operator token.
func (m *Module) withIngestAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if isLoopbackHTTP(r) {
			next(w, r)
			return
		}
		if !m.requireOperatorHTTPAuth(w, r) {
			return
		}
		next(w, r)
	}
}

func isLoopbackHTTP(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
