package internal

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func meshInsecure() bool {
	return os.Getenv("MUXCORE_INSECURE_DISABLE_TLS") == "true" || os.Getenv("MUXCORE_GRPC_INSECURE") == "true"
}

func peerDialOptions() ([]grpc.DialOption, error) {
	if meshInsecure() {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}
	creds, err := loadPeerTLS()
	if err != nil {
		return nil, err
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(creds)}, nil
}

func dialPeer(addr string) (*grpc.ClientConn, error) {
	opts, err := peerDialOptions()
	if err != nil {
		return nil, err
	}
	return grpc.NewClient(addr, opts...)
}

func loadPeerTLS() (credentials.TransportCredentials, error) {
	certFile := os.Getenv("MUXCORE_TLS_CERT")
	keyFile := os.Getenv("MUXCORE_TLS_KEY")
	caFile := os.Getenv("MUXCORE_TLS_CA")
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("TLS required — set MUXCORE_TLS_CERT/MUXCORE_TLS_KEY or MUXCORE_INSECURE_DISABLE_TLS=true")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load client TLS cert/key: %w", err)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if caFile != "" {
		pemBytes, err := os.ReadFile(caFile) //nolint:gosec // path from operator config
		if err != nil {
			return nil, fmt.Errorf("read TLS CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("parse TLS CA from %q", caFile)
		}
		tlsConfig.RootCAs = pool
	}
	return credentials.NewTLS(tlsConfig), nil
}
