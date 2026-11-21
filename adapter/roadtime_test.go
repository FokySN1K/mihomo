package adapter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A transport that keeps a session open (Hysteria2 keeps a QUIC connection,
// VLESS opens a gRPC or xhttp stream) dials locally once the session exists,
// so the check would report the local stream opening as the road to the node.
// The proxy must report the road time remembered from the handshake instead,
// and zero when this process has never seen the handshake.
func TestProxyRoadTimeWithoutHandshake(t *testing.T) {
	proxy := &Proxy{}

	require.Zero(t, proxy.roadTime(1), "a local dial on a fresh proxy has no road to report")
	require.Zero(t, proxy.roadTime(reusedDialMillis), "the threshold itself is a local dial")
	require.Equal(t, uint16(reusedDialMillis+1), proxy.roadTime(reusedDialMillis+1), "a dial beyond the threshold is a road time")
	require.Equal(t, uint16(reusedDialMillis+1), proxy.roadTime(1), "a later local dial reports the remembered road time")
}

func TestProxyRoadTimeKeepsTheLastHandshake(t *testing.T) {
	proxy := &Proxy{}

	require.Equal(t, uint16(30), proxy.roadTime(30), "the first handshake is reported as measured")
	require.Equal(t, uint16(30), proxy.roadTime(reusedDialMillis), "a local dial reports the handshake's road time")
	require.Equal(t, uint16(12), proxy.roadTime(12), "a new handshake replaces the memory")
	require.Equal(t, uint16(12), proxy.roadTime(1), "the newest handshake answers the next local dial")
}
