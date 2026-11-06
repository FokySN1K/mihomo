package adapter_test

import (
	"context"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"

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
	adapter.UrlTestHook = func(url string, name string, _ uint16, _ bool) {
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
}
