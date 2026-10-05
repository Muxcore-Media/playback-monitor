package internal

import (
	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/sdk/go/module/meshtls"
)

// meshInsecure reports whether the dev plaintext flag is set (ADR-0016/0017).
func meshInsecure() bool { return meshtls.Insecure() }

// dialPeer dials a mesh peer over mesh TLS unless the dev insecure flag is set.
func dialPeer(addr string) (*grpc.ClientConn, error) { return meshtls.Dial(addr) }
