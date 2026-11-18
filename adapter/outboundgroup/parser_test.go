package outboundgroup

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

func expandIncludeAll(t *testing.T, proxies []any, allProxies []string, filter string) []string {
	t.Helper()

	proxyMap := map[string]C.Proxy{"COMPATIBLE": namedProxy("COMPATIBLE")}
	for _, name := range allProxies {
		proxyMap[name] = namedProxy(name)
	}
	for _, name := range []string{"A", "B"} {
		proxyMap[name] = namedProxy(name)
	}

	config := map[string]any{
		"name":                "Group",
		"type":                "select",
		"proxies":             proxies,
		"include-all-proxies": true,
	}
	if filter != "" {
		config["filter"] = filter
	}

	parsed, err := ParseProxyGroup(config, proxyMap, map[string]P.ProxyProvider{}, allProxies, nil)
	require.NoError(t, err)

	names := make([]string, 0)
	for _, proxy := range parsed.Proxies() {
		names = append(names, proxy.Name())
	}
	return names
}

// The expansion is a set of nodes, not a second copy of what the profile
// already named: a node listed in `proxies` and matched by the filter must
// appear once.
func TestIncludeAllProxiesExpandsEachNodeOnce(t *testing.T) {
	names := expandIncludeAll(t, []any{"A", "hk-1"}, []string{"hk-1", "hk-2", "us-1"}, "(?i)hk")
	require.Equal(t, []string{"A", "hk-1", "hk-2"}, names)
}

// An explicit duplicate in `proxies` is a deliberate weight, so the expansion
// must not collapse what the profile wrote itself.
func TestIncludeAllProxiesKeepsExplicitDuplicates(t *testing.T) {
	names := expandIncludeAll(t, []any{"A", "A"}, []string{"A", "B"}, "")
	require.Equal(t, []string{"A", "A", "B"}, names,
		"only the expanded names are deduplicated")
}
