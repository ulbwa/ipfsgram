// autotls.go — AutoTLS via p2p-forge (libp2p.direct), mirroring Kubo's config.
// The cert manager provisions a browser-trusted TLS certificate for
// <peerid>.libp2p.direct over ACME DNS-01 and advertises the matching secure
// WebSocket (WSS) addresses, so the node becomes dialable from browsers and
// public gateways. Everything here is best-effort and asynchronous: ACME runs
// in a background goroutine started by certMgr.Start() and never blocks or
// fails node startup if cert provisioning is slow or unavailable.

package node

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/caddyserver/certmagic"
	forge "github.com/ipshipyard/p2p-forge/client"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	libp2pwebrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	libp2pwebsocket "github.com/libp2p/go-libp2p/p2p/transport/websocket"
	libp2pwebtransport "github.com/libp2p/go-libp2p/p2p/transport/webtransport"
	"github.com/rs/zerolog/log"
	"go.uber.org/zap"
)

// newForgeCertMgr builds the p2p-forge certificate manager, mirroring Kubo's
// core/node/libp2p P2PForgeCertMgr wiring. Certificates are persisted under
// <dataDir>/p2p-forge-certs so they survive restarts. The manager is created
// here but only begins ACME work when Start() is called after the host exists.
func newForgeCertMgr(dataDir string) (*forge.P2PForgeCertMgr, error) {
	storagePath := filepath.Join(dataDir, forge.DefaultStorageLocation)
	// Route certmagic's own loggers to a discard zap logger so its noisy ACME
	// output does not pollute the daemon's structured logs; p2p-forge logs the
	// salient events through WithLogger below.
	rawLogger := zap.NewNop()
	certmagic.Default.Logger = rawLogger
	certmagic.DefaultACME.Logger = rawLogger

	return forge.NewP2PForgeCertMgr(
		forge.WithLogger(rawLogger.Sugar()),
		forge.WithForgeDomain(forge.DefaultForgeDomain),
		forge.WithForgeRegistrationEndpoint(forge.DefaultForgeEndpoint),
		// Explicitly enabled by config, so register immediately (no delay), the
		// same as Kubo when AutoTLS.Enabled is set to true.
		forge.WithRegistrationDelay(0*time.Second),
		forge.WithCAEndpoint(forge.DefaultCAEndpoint),
		forge.WithUserAgent("ipfsgram"),
		forge.WithCertificateStorage(&certmagic.FileStorage{Path: storagePath}),
		forge.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
	)
}

// forgeTransportOptions returns the full transport set for the AutoTLS case.
// We cannot use libp2p.DefaultTransports here: it already registers a plain
// WebSocket transport, and adding a second (TLS-configured) one fails with
// "transports already registered for protocol(s): ws, wss". So we enumerate the
// default transports explicitly (tcp, quic, webtransport, webrtc-direct) and
// substitute the WebSocket transport with one wired to the forge cert manager's
// TLS config, which serves WSS on the libp2p.direct SNI.
func forgeTransportOptions(certMgr *forge.P2PForgeCertMgr) []libp2p.Option {
	return []libp2p.Option{
		libp2p.Transport(libp2ptcp.NewTCPTransport),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Transport(libp2pwebtransport.New),
		libp2p.Transport(libp2pwebrtc.New),
		libp2p.Transport(libp2pwebsocket.New, libp2pwebsocket.WithTLSConfig(certMgr.TLSConfig())),
	}
}

// startForgeCertMgr binds the host to the cert manager and kicks off ACME in
// the background. It never returns an error that should be fatal: a failure to
// start cert management only means the node will not have a libp2p.direct cert
// yet, while it keeps serving over DHT, bitswap and the other transports.
func startForgeCertMgr(certMgr *forge.P2PForgeCertMgr, h host.Host) {
	certMgr.ProvideHost(h)
	if err := certMgr.Start(); err != nil {
		log.Warn().Err(err).Msg("autotls: cert manager failed to start; continuing without libp2p.direct")
		return
	}
	log.Info().Msg("autotls: p2p-forge cert manager started (libp2p.direct certificate provisioning in background)")
}
