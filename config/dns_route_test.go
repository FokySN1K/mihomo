package config

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	RC "github.com/metacubex/mihomo/rules/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func routeModeRawConfig(filter []string) *RawConfig {
	rawCfg := DefaultRawConfig()
	rawCfg.DNS.Enable = true
	rawCfg.DNS.EnhancedMode = C.DNSFakeIP
	rawCfg.DNS.FakeIPRange = "198.18.0.1/16"
	rawCfg.DNS.DefaultNameserver = []string{"1.1.1.1"}
	rawCfg.DNS.NameServer = []string{"1.1.1.1"}
	rawCfg.DNS.FakeIPFilterMode = C.FilterRoute
	rawCfg.DNS.FakeIPFilter = filter
	return rawCfg
}

func TestParseDNSRouteModeUsesRules(t *testing.T) {
	rules := []C.Rule{
		RC.NewDomainSuffix("example.com", "DIRECT"),
		RC.NewMatch("Proxy"),
	}

	dnsCfg, err := parseDNS(routeModeRawConfig(nil), nil, rules)
	require.NoError(t, err)
	require.NotNil(t, dnsCfg.FakeIPSkipper)

	// covered by a DIRECT rule, so the client should see the real address
	assert.True(t, dnsCfg.FakeIPSkipper.ShouldSkipped("example.com"))
	// proxied by MATCH, so the domain has to survive in the fake address
	assert.False(t, dnsCfg.FakeIPSkipper.ShouldSkipped("google.com"))
}

func TestParseDNSRouteModeRejectsFilter(t *testing.T) {
	_, err := parseDNS(routeModeRawConfig([]string{"*.lan"}), nil, nil)
	require.Error(t, err)
}
