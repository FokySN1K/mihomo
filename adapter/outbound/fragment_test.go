package outbound

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The provider publishes every REALITY node with a bare IP as the server name
// only in the "2 способ" set, together with xray's `fm`; the split must follow
// the name, not the node.
func TestIsIPServerName(t *testing.T) {
	cases := []struct {
		serverName string
		server     string
		ip         bool
	}{
		{"168.113.131.162", "168.113.131.162", true},
		{"", "168.113.131.162", true},
		{"[2a00::1]", "2a00::1", true},
		{"", "2a00::1", true},
		{"xyeta.cloude.one", "1.2.3.4", false},
		{"des.cloude.one", "des.cloude.one", false},
		{"", "example.com", false},
		{"", "", false},
	}

	for _, item := range cases {
		require.Equal(t, item.ip, isIPServerName(item.serverName, item.server),
			"servername %q, server %q", item.serverName, item.server)
	}
}

// Only REALITY with an IP name asks for the split, and the preset is the one
// the subscriptions carry in `fm`: 10-20 bytes a piece, no delay, ClientHello
// only.
func TestAutoFragmentForReality(t *testing.T) {
	require.Nil(t, autoFragmentForReality("domain", true, "des.cloude.one", "1.2.3.4"),
		"a domain server name needs no split")
	require.Nil(t, autoFragmentForReality("plain", false, "1.2.3.4", "1.2.3.4"),
		"a non-REALITY node is left alone")

	config := autoFragmentForReality("ip", true, "1.2.3.4", "1.2.3.4")
	require.NotNil(t, config)
	require.True(t, config.Enabled)
	require.Equal(t, "10-20", config.Length)
	require.Equal(t, "0", config.Delay)
	require.Equal(t, "tlshello", config.Packets)
}
