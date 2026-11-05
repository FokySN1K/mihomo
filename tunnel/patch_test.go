package tunnel

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

// fakeProxy answers only what the direct-name snapshot asks; every other method
// of the interface stays nil behind the embedded field.
type fakeProxy struct {
	C.Proxy
	name string
	typ  C.AdapterType
}

func (p fakeProxy) Name() string        { return p.name }
func (p fakeProxy) Type() C.AdapterType { return p.typ }

type fakeProvider struct {
	P.ProxyProvider
	name    string
	proxies []C.Proxy
	version uint32
}

func (p *fakeProvider) Name() string       { return p.name }
func (p *fakeProvider) Proxies() []C.Proxy { return p.proxies }
func (p *fakeProvider) Version() uint32    { return p.version }

func (p *fakeProvider) set(proxies ...C.Proxy) {
	p.proxies = proxies
	p.version++
}

// TestIsDirectBeforeAnyApply guards the state statistics sees on a core that
// has not applied a config yet: only the built-in adapter is known.
func TestIsDirectBeforeAnyApply(t *testing.T) {
	directNames.Store(nil)
	defer directNames.Store(nil)

	require.True(t, IsDirect("DIRECT"))
	require.False(t, IsDirect("lan"))
}

// TestDirectNamesFollowProviderChange guards every source of the snapshot: the
// apply, a provider that already carries a list at apply time, and a provider
// that installs or replaces its list later.
func TestDirectNamesFollowProviderChange(t *testing.T) {
	configDirect := fakeProxy{name: "DIRECT", typ: C.Direct}
	payloadDirect := fakeProxy{name: "payload", typ: C.Direct}
	lateDirect := fakeProxy{name: "late", typ: C.Direct}
	node := fakeProxy{name: "node", typ: C.Shadowsocks}

	preloaded := &fakeProvider{name: "preloaded", proxies: []C.Proxy{payloadDirect}}
	loading := &fakeProvider{name: "loading"}

	UpdateProxies(
		map[string]C.Proxy{"DIRECT": configDirect, "node": node},
		map[string]P.ProxyProvider{"preloaded": preloaded, "loading": loading},
	)

	require.True(t, IsDirect("DIRECT"), "a direct proxy of the config is known at once")
	require.True(t, IsDirect("payload"), "a provider that already carries a list is part of the apply")
	require.False(t, IsDirect("node"), "a proxied outbound is not direct")
	require.False(t, IsDirect("late"), "a provider that loads after the apply is not seen yet")

	loading.set(lateDirect)
	P.NotifyChanged("loading")
	require.True(t, IsDirect("late"), "a provider list that arrived after the apply must be seen")

	loading.set(node)
	P.NotifyChanged("loading")
	require.False(t, IsDirect("late"), "a refresh that drops the direct proxy must update the set")
}
