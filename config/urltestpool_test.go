package config

import (
	"testing"

	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/adapter/outboundgroup/probequeue"
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

// `ordered` reaches the pool through the group decoder, including 0, and a
// pool that set none keeps a nil place for the declaration order.
func TestParseURLTestPoolOrderedOption(t *testing.T) {
	t.Cleanup(probequeue.ResetChain)
	cfg := &RawConfig{
		Proxy: []map[string]any{
			{"name": "p1", "type": "socks5", "server": "127.0.0.1", "port": 1080},
		},
		ProxyGroup: []map[string]any{
			{
				"name":    "second",
				"type":    "url-test-pool",
				"proxies": []string{"p1"},
				"ordered": 2,
			},
			{
				"name":    "first",
				"type":    "url-test-pool",
				"proxies": []string{"p1"},
				"ordered": 0,
			},
			{
				"name":    "plain",
				"type":    "url-test-pool",
				"proxies": []string{"p1"},
			},
		},
	}

	proxies, _, err := parseProxies(cfg)
	require.NoError(t, err)

	second, ok := proxies["second"].Adapter().(*outboundgroup.URLTestPool)
	require.True(t, ok)
	require.Equal(t, 2, *second.ChainOrder())
	first, ok := proxies["first"].Adapter().(*outboundgroup.URLTestPool)
	require.True(t, ok)
	require.Equal(t, 0, *first.ChainOrder())
	plain, ok := proxies["plain"].Adapter().(*outboundgroup.URLTestPool)
	require.True(t, ok)
	require.Nil(t, plain.ChainOrder(),
		"a pool without ordered takes its place from the declaration order")
}

func TestParseURLTestPoolRejectsNegativeOrdered(t *testing.T) {
	cfg := &RawConfig{
		Proxy: []map[string]any{
			{"name": "p1", "type": "socks5", "server": "127.0.0.1", "port": 1080},
		},
		ProxyGroup: []map[string]any{
			{
				"name":    "pool",
				"type":    "url-test-pool",
				"proxies": []string{"p1"},
				"ordered": -1,
			},
		},
	}

	_, _, err := parseProxies(cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, "ordered")
}

// `probe-order` is the profile-wide switch of the chain: sequential by
// default, parallel restores the old urgent round-robin, and anything else is
// a config error instead of a silent fallback.
func TestParseProbeOrderOption(t *testing.T) {
	general, err := parseGeneral(&RawConfig{ProbeOrder: "parallel"})
	require.NoError(t, err)
	require.Equal(t, "parallel", general.ProbeOrder)

	general, err = parseGeneral(&RawConfig{ProbeOrder: "SEQUENTIAL"})
	require.NoError(t, err)
	require.Equal(t, "sequential", general.ProbeOrder, "the value must be normalized for the API")

	general, err = parseGeneral(&RawConfig{})
	require.NoError(t, err)
	require.Equal(t, "sequential", general.ProbeOrder, "sequential is the default")

	_, err = parseGeneral(&RawConfig{ProbeOrder: "random"})
	require.Error(t, err)
	require.ErrorContains(t, err, "probe-order")
}
