package internal

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Muxcore-Media/core/sdk/go/module/netguard"
)

// tautulliGuardOpts: Tautulli is an admin-configured integration, usually on
// the LAN or loopback; metadata/link-local targets stay blocked.
var tautulliGuardOpts = netguard.Options{AllowPrivate: true, AllowLoopback: true, Timeout: 60 * time.Second}

var tautulliHTTPClient = netguard.NewClient(netguard.Integration, tautulliGuardOpts)

func validateOutboundHTTPURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("url required")
	}
	if err := netguard.ValidateURL(raw, netguard.Integration, tautulliGuardOpts); err != nil {
		return nil, err
	}
	return url.Parse(raw)
}
