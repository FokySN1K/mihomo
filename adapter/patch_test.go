package adapter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/utils"

	"github.com/stretchr/testify/require"
)

// A host that shows which members are being measured needs the start of the
// probe, not only its result: a member whose probe has not returned yet would
// otherwise look idle to it.
func TestProxyURLTestReportsProbeStart(t *testing.T) {
	proxy := adapter.NewProxy(outbound.NewDirect())

	var started []string
	var finished []string
	adapter.UrlTestStartHook = func(url string, name string) {
		started = append(started, url+" "+name)
	}
	var sawRtt uint16
	adapter.UrlTestHook = func(url string, name string, _ uint16, rtt uint16, _ bool) {
		sawRtt = rtt
		finished = append(finished, url+" "+name)
	}
	t.Cleanup(func() {
		adapter.UrlTestStartHook = nil
		adapter.UrlTestHook = nil
	})

	// nothing listens on port 1, so the probe fails at the dial; the start is
	// reported before that, which is the point of the hook
	_, err := proxy.URLTest(context.Background(), "http://127.0.0.1:1/", nil)
	require.Error(t, err)

	want := []string{"http://127.0.0.1:1/ " + proxy.Name()}
	require.Equal(t, want, started, "the start of the probe must be reported")
	require.Equal(t, want, finished, "a failed probe must still report its result")
	require.Zero(t, sawRtt, "a probe that never reached the proxy has no road time to report")
}

// A host that shows the road to a node apart from the server time needs the
// dial time on every result, not only on the ones that failed. A dial to the
// host itself never crosses the network, so it is no road time: the hook gets
// zero, the "not measured" value, instead of a near-zero figure that would
// read as a very fast node.
func TestProxyURLTestReportsRoadTime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	expected, err := utils.NewUnsignedRanges[uint16]("204")
	require.NoError(t, err)
	proxy := adapter.NewProxy(outbound.NewDirect())

	var delay, rtt uint16
	adapter.UrlTestHook = func(_ string, _ string, d uint16, r uint16, _ bool) {
		delay, rtt = d, r
	}
	t.Cleanup(func() {
		adapter.UrlTestHook = nil
	})

	got, err := proxy.URLTest(context.Background(), server.URL, expected)
	require.NoError(t, err)
	require.Equal(t, got, delay, "the hook must carry the measured delay")
	require.Zero(t, rtt, "a dial that never left the host measures no road")
}
