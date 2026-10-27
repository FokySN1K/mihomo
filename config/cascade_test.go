package config

import (
	"testing"

	"github.com/metacubex/mihomo/adapter/outboundgroup"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func TestParseCascadeGroup(t *testing.T) {
	cfg := &RawConfig{
		Proxy: []map[string]any{
			{"name": "p1", "type": "socks5", "server": "127.0.0.1", "port": 1080},
			{"name": "p2", "type": "socks5", "server": "127.0.0.1", "port": 1081},
		},
		ProxyGroup: []map[string]any{
			{
				"name":            "cascade",
				"type":            "cascade",
				"proxies":         []string{"p1", "p2"},
				"url":             "https://example.com/generate_204",
				"interval":        120,
				"lookahead":       1,
				"max-attempts":    2,
				"failed-cooldown": 15,
			},
		},
	}

	proxies, _, err := parseProxies(cfg)
	require.NoError(t, err)

	group, ok := proxies["cascade"].Adapter().(outboundgroup.ProxyGroup)
	require.True(t, ok)
	require.Equal(t, C.Cascade, group.Type())
}

func TestParseCascadeAcceptsNestedGroup(t *testing.T) {
	cfg := &RawConfig{
		Proxy: []map[string]any{
			{"name": "p1", "type": "socks5", "server": "127.0.0.1", "port": 1080},
		},
		ProxyGroup: []map[string]any{
			{
				"name":    "pool",
				"type":    "url-test-pool",
				"proxies": []string{"p1"},
				"url":     "https://example.com/generate_204",
			},
			{
				"name":    "cascade",
				"type":    "cascade",
				"proxies": []string{"pool"},
				"url":     "https://example.com/generate_204",
			},
		},
	}

	proxies, _, err := parseProxies(cfg)
	require.NoError(t, err)

	group, ok := proxies["cascade"].Adapter().(outboundgroup.ProxyGroup)
	require.True(t, ok)
	require.Equal(t, C.Cascade, group.Type())
	require.Equal(t, "pool", group.Now())
}
