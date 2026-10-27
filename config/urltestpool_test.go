package config

import (
	"testing"

	"github.com/metacubex/mihomo/adapter/outboundgroup"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func TestParseURLTestPoolGroup(t *testing.T) {
	cfg := &RawConfig{
		Proxy: []map[string]any{
			{"name": "p1", "type": "socks5", "server": "127.0.0.1", "port": 1080},
			{"name": "p2", "type": "socks5", "server": "127.0.0.1", "port": 1081},
		},
		ProxyGroup: []map[string]any{
			{
				"name":            "pool",
				"type":            "url-test-pool",
				"proxies":         []string{"p1", "p2"},
				"url":             "https://example.com/generate_204",
				"interval":        300,
				"full-interval":   1800,
				"pool-size":       1,
				"degraded-checks": 1,
			},
		},
	}

	proxies, _, err := parseProxies(cfg)
	require.NoError(t, err)

	group, ok := proxies["pool"].Adapter().(outboundgroup.ProxyGroup)
	require.True(t, ok)
	require.Equal(t, C.URLTestPool, group.Type())
}

func TestParseURLTestPoolRejectsNestedGroup(t *testing.T) {
	cfg := &RawConfig{
		Proxy: []map[string]any{
			{"name": "p1", "type": "socks5", "server": "127.0.0.1", "port": 1080},
		},
		ProxyGroup: []map[string]any{
			{
				"name":    "inner",
				"type":    "url-test",
				"proxies": []string{"p1"},
				"url":     "https://example.com/generate_204",
			},
			{
				"name":    "pool",
				"type":    "url-test-pool",
				"proxies": []string{"inner"},
				"url":     "https://example.com/generate_204",
			},
		},
	}

	_, _, err := parseProxies(cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, "can't contain proxy group")
}
