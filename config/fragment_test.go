package config

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func TestParseFragmentOutbound(t *testing.T) {
	cfg := &RawConfig{
		Proxy: []map[string]any{
			{
				"name":           "yt-fragment",
				"type":           "fragment",
				"mode":           "tls",
				"segments":       3,
				"delay":          15,
				"split-position": 3,
			},
		},
	}

	proxies, _, err := parseProxies(cfg)
	require.NoError(t, err)

	proxy, ok := proxies["yt-fragment"]
	require.True(t, ok)
	require.Equal(t, C.Fragment, proxy.Type())
	require.True(t, proxy.SupportUDP())
}

func TestParseFragmentRejectsUnknownMode(t *testing.T) {
	cfg := &RawConfig{
		Proxy: []map[string]any{
			{"name": "yt-fragment", "type": "fragment", "mode": "quic"},
		},
	}

	_, _, err := parseProxies(cfg)
	require.ErrorContains(t, err, "unsupported mode")
}
