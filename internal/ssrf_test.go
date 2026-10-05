package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// allowLocalWebhooks lets tests deliver to loopback httptest servers by
// swapping the guarded webhook validator/client; restored on cleanup.
func allowLocalWebhooks(t *testing.T) {
	t.Helper()
	oldV, oldC := webhookValidate, webhookHTTPClient
	webhookValidate = func(string) error { return nil }
	webhookHTTPClient = &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(func() { webhookValidate, webhookHTTPClient = oldV, oldC })
}

func TestWebhookURLsBlocked(t *testing.T) {
	for _, u := range []string{
		"http://hooks.example.com/x", "https://127.0.0.1/x", "https://localhost/x",
		"https://10.0.0.5/x", "https://192.168.1.1/x", "https://172.20.0.1/x",
		"https://169.254.169.254/latest/meta-data", "https://[::1]/x", "https://[fd00::1]/x",
		"https://metadata.google.internal/x", "https://0x7f000001/x", "https://printer.lan/x",
		"ftp://example.com/x",
	} {
		if err := assertSafeWebhookURL(u); err == nil {
			t.Errorf("accepted %q", u)
		}
	}
	if err := assertSafeWebhookURL("https://discord.com/api/webhooks/1/x"); err != nil {
		t.Errorf("public https rejected: %v", err)
	}
}

func TestLocalWebhookEnvBypassRemoved(t *testing.T) {
	t.Setenv("PLAYBACK_MONITOR_ALLOW_LOCAL_WEBHOOKS", "1")
	if err := assertSafeWebhookURL("https://127.0.0.1/hook"); err == nil {
		t.Fatal("env bypass still honoured")
	}
}

func TestValidateDestinationConfigRejectsPrivate(t *testing.T) {
	for _, typ := range []string{"discord", "slack", "webhook"} {
		if err := validateDestinationConfig(typ, map[string]string{"webhook_url": "https://10.0.0.1/x"}); err == nil {
			t.Errorf("%s: private accepted", typ)
		}
	}
}

func TestWebhookClientBlocksPrivateAtDial(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("request reached loopback server")
	}))
	defer srv.Close()
	m := &Module{}
	err := m.postWebhookJSON(context.Background(), srv.URL, []byte(`{}`))
	if err == nil {
		t.Fatal("expected loopback webhook to be blocked")
	}
}

func TestAppriseBaseMetadataBlockedLANAllowed(t *testing.T) {
	m := &Module{}
	t.Setenv("PLAYBACK_MONITOR_APPRISE_URL", "http://169.254.169.254")
	if err := m.postAppriseURLs(context.Background(), "mailto://x", "t", "m", "info"); err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("metadata apprise base not rejected: %v", err)
	}
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit = true }))
	defer srv.Close()
	t.Setenv("PLAYBACK_MONITOR_APPRISE_URL", srv.URL) // loopback = legitimate local apprise
	if err := m.postAppriseURLs(context.Background(), "mailto://x", "t", "m", "info"); err != nil || !hit {
		t.Fatalf("loopback apprise should work: err=%v hit=%v", err, hit)
	}
}

func TestTautulliURLGuard(t *testing.T) {
	for _, u := range []string{"http://169.254.169.254/", "http://metadata.google.internal/", "file:///etc/passwd", "http://[fe80::1]:8181"} {
		if _, err := validateOutboundHTTPURL(u); err == nil {
			t.Errorf("accepted %q", u)
		}
	}
	for _, u := range []string{"http://127.0.0.1:8181", "http://192.168.1.10:8181", "http://tautulli:8181", "https://tautulli.example.com"} {
		if _, err := validateOutboundHTTPURL(u); err != nil {
			t.Errorf("rejected %q: %v", u, err)
		}
	}
	_, _, err := fetchTautulliHistoryPage(context.Background(), "http://169.254.169.254", "k", 0, 10)
	if err == nil {
		t.Fatal("fetch to metadata IP succeeded")
	}
}
