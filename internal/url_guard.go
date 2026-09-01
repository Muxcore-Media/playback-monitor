package internal

import (
	"fmt"
	"net/url"
	"strings"
)

var blockedMetadataHosts = map[string]struct{}{
	"169.254.169.254":           {},
	"metadata.google.internal":  {},
	"metadata.goog":             {},
	"metadata.google.internal.": {},
}

func validateOutboundHTTPURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("url required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("url scheme must be http or https")
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if host == "" {
		return nil, fmt.Errorf("url host required")
	}
	if _, blocked := blockedMetadataHosts[host]; blocked {
		return nil, fmt.Errorf("url host %q is not allowed", host)
	}
	return u, nil
}
