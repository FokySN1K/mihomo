package route

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/adapter/provider"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/require"
)

func directProxy(name string) C.Proxy {
	return adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: name}))
}

// A delay test measures the members; it is not a manual selection, and the pin
// the user set must survive it. The test URL points at a closed local port, so
// the measurement fails at once instead of touching the network.
func TestGetGroupDelayKeepsTheManualPin(t *testing.T) {
	member := directProxy("node")
	pd, err := provider.NewCompatibleProvider("group", []C.Proxy{member},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	group, err := outboundgroup.NewURLTest(
		outboundgroup.GroupCommonOption{Name: "group", URL: "http://127.0.0.1:1/"},
		outboundgroup.URLTestOption{},
		directProxy("COMPATIBLE"),
		[]P.ProxyProvider{pd},
	)
	require.NoError(t, err)
	group.ForceSet("node")

	proxy := adapter.NewProxy(group)
	request := httptest.NewRequest(http.MethodGet, "/group/delay?url=http://127.0.0.1:1/&timeout=500", nil)
	request = request.WithContext(context.WithValue(request.Context(), CtxKeyProxy, proxy))

	getGroupDelay(httptest.NewRecorder(), request)

	data, err := group.MarshalJSON()
	require.NoError(t, err)
	decoded := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, "node", decoded["fixed"], "a delay test must not clear the pin")
}
