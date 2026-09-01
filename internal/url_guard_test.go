package internal

import (
	"testing"
)

func TestValidateOutboundHTTPURLRejectsMetadataHost(t *testing.T) {
	_, err := validateOutboundHTTPURL("http://169.254.169.254/latest/meta-data/")
	if err == nil {
		t.Fatal("expected metadata host to be rejected")
	}
	_, err = validateOutboundHTTPURL("http://metadata.google.internal/computeMetadata/v1/")
	if err == nil {
		t.Fatal("expected google metadata host to be rejected")
	}
	_, err = validateOutboundHTTPURL("ftp://tautulli.local/api")
	if err == nil {
		t.Fatal("expected non-http scheme to be rejected")
	}
	u, err := validateOutboundHTTPURL("http://192.168.1.10:8181")
	if err != nil || u.Host != "192.168.1.10:8181" {
		t.Fatalf("expected LAN url allowed, got %v err=%v", u, err)
	}
}
