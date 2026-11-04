package executor

import (
	"net"
	"testing"

	"github.com/metacubex/mihomo/config"
	"github.com/metacubex/mihomo/listener"
)

// TestApplyInboundListenersOpensThePort guards the standalone run: a build that
// never opts out must open mixed-port from ApplyConfig, which is exactly what an
// embedded core had turned off for every host.
func TestApplyInboundListenersOpensThePort(t *testing.T) {
	listener.StopListener()
	t.Cleanup(listener.StopListener)

	port := freeTCPPort(t)
	cfg := &config.Config{General: &config.General{Inbound: config.Inbound{MixedPort: port}}}
	applyInboundListeners(cfg, true)

	if got := listener.GetPorts().MixedPort; got != port {
		t.Fatalf("mixed-port = %d, want %d", got, port)
	}
}

// TestEmbeddedCoreLeavesThePortClosed is the other half of the contract: the
// FlClash core opens the inbounds from its own lifecycle, so ApplyConfig must
// not bind them behind its back.
func TestEmbeddedCoreLeavesThePortClosed(t *testing.T) {
	listener.StopListener()
	t.Cleanup(func() {
		embedded.Store(false)
		listener.StopListener()
	})
	embedded.Store(true)

	port := freeTCPPort(t)
	cfg := &config.Config{General: &config.General{Inbound: config.Inbound{MixedPort: port}}}
	applyInboundListeners(cfg, true)

	if got := listener.GetPorts().MixedPort; got != 0 {
		t.Fatalf("embedded core opened mixed-port %d", got)
	}
}

// freeTCPPort asks the kernel for a port nothing listens on right now, so the
// test can bind it without colliding with another run.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
