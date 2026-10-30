package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

type cascadeFakeAdapter struct {
	*outbound.Base
	dialFn func(ctx context.Context, metadata *C.Metadata) (C.Conn, error)
}

func (f *cascadeFakeAdapter) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	return f.dialFn(ctx, metadata)
}

// cascadeFakeProxy overrides URLTest so group checks don't need a real network.
type cascadeFakeProxy struct {
	*adapter.Proxy
	testErr error
	delay   uint16
}

func (f *cascadeFakeProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	if f.testErr != nil {
		return 0, f.testErr
	}
	return f.delay, nil
}

type cascadeFakeConn struct {
	N.ExtendedConn
}

func (c *cascadeFakeConn) Chains() C.Chain               { return nil }
func (c *cascadeFakeConn) ProviderChains() C.Chain       { return nil }
func (c *cascadeFakeConn) AppendToChains(C.ProxyAdapter) {}
func (c *cascadeFakeConn) RemoteDestination() string     { return "" }

type dialLog struct {
	mu    sync.Mutex
	names []string
}

func (l *dialLog) record(name string) {
	l.mu.Lock()
	l.names = append(l.names, name)
	l.mu.Unlock()
}

func (l *dialLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.names...)
}

func (l *dialLog) reset() {
	l.mu.Lock()
	l.names = nil
	l.mu.Unlock()
}

func newCascadeFakeProxy(name string, adapterType C.AdapterType, dialFn func(context.Context, *C.Metadata) (C.Conn, error), testErr error, delay uint16) *cascadeFakeProxy {
	if dialFn == nil {
		dialFn = func(context.Context, *C.Metadata) (C.Conn, error) {
			return nil, errors.New("dial failed")
		}
	}
	base := outbound.NewBase(outbound.BaseOption{Name: name, Type: adapterType, UDP: true})
	return &cascadeFakeProxy{
		Proxy:   adapter.NewProxy(&cascadeFakeAdapter{Base: base, dialFn: dialFn}),
		testErr: testErr,
		delay:   delay,
	}
}

func newCascadeTestProxy(name string) *cascadeFakeProxy {
	return newCascadeFakeProxy(name, C.Shadowsocks, nil, nil, 100)
}

func newCascadeFailingProxy(name string, log *dialLog) *cascadeFakeProxy {
	return newCascadeFakeProxy(name, C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
		log.record(name)
		return nil, errors.New("dial failed")
	}, nil, 0)
}

func newCascadeSucceedingProxy(name string, log *dialLog) *cascadeFakeProxy {
	return newCascadeFakeProxy(name, C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
		log.record(name)
		return &cascadeFakeConn{}, nil
	}, nil, 0)
}

func intPtr(v int) *int {
	return &v
}

// newCascadeForTest builds a group and freezes its scheduler, so unit tests
// never race with an automatically started health check.
func newCascadeForTest(t *testing.T, opt CascadeOption, proxies ...C.Proxy) *Cascade {
	t.Helper()

	pd, err := provider.NewCompatibleProvider("cascade", proxies,
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	group, err := NewCascade(GroupCommonOption{Name: "cascade", Interval: 300},
		opt, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	group.lastCheck.Store(time.Now())
	return group
}

func cascadeProxyNames(proxies []C.Proxy) []string {
	names := make([]string, 0, len(proxies))
	for _, proxy := range proxies {
		names = append(names, proxy.Name())
	}
	return names
}

func TestCascadeDefaults(t *testing.T) {
	group, err := NewCascade(GroupCommonOption{Name: "cascade"}, CascadeOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	require.Equal(t, C.Cascade, group.Type())
	require.Equal(t, 2, group.lookahead)
	require.Equal(t, 3, group.maxAttempts)
	require.Equal(t, 30*time.Second, group.failedCooldown)
	require.Equal(t, 300*time.Second, group.interval)
	require.Equal(t, C.DefaultTestURL, group.testUrl)

	_, err = NewCascade(GroupCommonOption{Name: "cascade"}, CascadeOption{}, nil, nil)
	require.ErrorContains(t, err, "empty fallback proxy not exist")
}

func TestCascadeExplicitOptions(t *testing.T) {
	group, err := NewCascade(GroupCommonOption{Name: "cascade", Interval: 60},
		CascadeOption{
			Lookahead:      intPtr(-1),
			MaxAttempts:    intPtr(0),
			FailedCooldown: intPtr(0),
		}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	require.Zero(t, group.lookahead)
	require.Equal(t, 1, group.maxAttempts)
	require.Zero(t, group.failedCooldown)
	require.Equal(t, 60*time.Second, group.interval)

	group, err = NewCascade(GroupCommonOption{Name: "cascade"},
		CascadeOption{
			Lookahead:      intPtr(4),
			MaxAttempts:    intPtr(7),
			FailedCooldown: intPtr(15),
		}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	require.Equal(t, 4, group.lookahead)
	require.Equal(t, 7, group.maxAttempts)
	require.Equal(t, 15*time.Second, group.failedCooldown)
}

func TestCascadeAcceptsNestedGroup(t *testing.T) {
	emptyFallback := newPoolEmptyFallback()
	inner, err := NewURLTest(GroupCommonOption{Name: "inner", URL: poolTestUrl}, URLTestOption{}, emptyFallback, nil)
	require.NoError(t, err)

	groupProxy := adapter.NewProxy(inner)
	pd, err := provider.NewCompatibleProvider("cascade", []C.Proxy{groupProxy},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	group, err := NewCascade(GroupCommonOption{Name: "cascade"}, CascadeOption{}, emptyFallback, []P.ProxyProvider{pd})
	require.NoError(t, err)

	proxies := group.GetProxies(false)
	require.Len(t, proxies, 1)
	require.Equal(t, "inner", proxies[0].Name())
}

func TestCascadeCandidateOrder(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{},
		newCascadeTestProxy("a"),
		newCascadeTestProxy("b"),
		newCascadeTestProxy("c"),
		newCascadeTestProxy("d"),
	)

	now := time.Now()
	group.mu.Lock()
	group.states["b"] = &cascadeState{failedUntil: now.Add(time.Minute)}
	group.states["c"] = &cascadeState{alive: true}
	group.states["d"] = &cascadeState{failedUntil: now.Add(time.Minute)}
	group.active = "b"
	group.selected = "d"
	group.mu.Unlock()

	// both the pinned and the active candidates are cooling down, so the warm
	// ones go first and the failed ones are pushed to the end
	order := group.candidateOrder(group.GetProxies(false))
	require.Equal(t, []string{"c", "a", "b", "d"}, cascadeProxyNames(order))

	group.mu.Lock()
	group.states["d"].failedUntil = time.Time{}
	group.states["b"].failedUntil = time.Time{}
	group.mu.Unlock()

	// a pinned candidate jumps the queue, the active one stays right after it
	order = group.candidateOrder(group.GetProxies(false))
	require.Equal(t, []string{"d", "b", "c", "a"}, cascadeProxyNames(order))
}

func TestCascadeCandidateOrderPromotesLastActiveAfterOutage(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{},
		newCascadeTestProxy("ru"),
		newCascadeTestProxy("pool"),
		newCascadeTestProxy("other"),
	)

	now := time.Now()
	group.mu.Lock()
	group.states["ru"] = &cascadeState{failedUntil: now.Add(time.Minute)}
	group.states["pool"] = &cascadeState{failedUntil: now.Add(time.Minute)}
	group.states["other"] = &cascadeState{failedUntil: now.Add(time.Minute)}
	group.lastActive = "pool"
	group.mu.Unlock()

	// every candidate is cooling down: the last active one gets the first chance
	order := group.candidateOrder(group.GetProxies(false))
	require.Equal(t, []string{"pool", "ru", "other"}, cascadeProxyNames(order))
}

func TestCascadeLastActiveDoesNotPreemptActive(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{}, newCascadeTestProxy("a"), newCascadeTestProxy("b"))

	group.mu.Lock()
	group.active = "a"
	group.lastActive = "b"
	group.states["a"] = &cascadeState{alive: true}
	group.states["b"] = &cascadeState{alive: true}
	group.mu.Unlock()

	order := group.candidateOrder(group.GetProxies(false))
	require.Equal(t, []string{"a", "b"}, cascadeProxyNames(order))
}

func TestCascadeRemembersLastActive(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{}, newCascadeTestProxy("a"), newCascadeTestProxy("b"))

	group.onAttemptSucceeded("b")
	group.mu.RLock()
	require.Equal(t, "b", group.lastActive)
	group.mu.RUnlock()

	group.markUnavailable("b")
	group.mu.RLock()
	require.Empty(t, group.active)
	require.Equal(t, "b", group.lastActive, "a failure must not forget the last active candidate")
	group.mu.RUnlock()
}

func TestCascadeSettleActive(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{}, newCascadeTestProxy("a"), newCascadeTestProxy("b"))
	proxies := group.GetProxies(false)

	group.mu.Lock()
	group.states["a"] = &cascadeState{alive: true}
	group.states["b"] = &cascadeState{alive: true}
	group.active = "b"
	group.mu.Unlock()

	// a recovered higher-priority candidate never preempts the active one
	group.settleActive(proxies)
	require.Equal(t, "b", group.active)

	group.mu.Lock()
	group.states["b"].alive = false
	group.mu.Unlock()

	group.settleActive(proxies)
	require.Equal(t, "a", group.active)

	// but an explicit choice always wins
	group.ForceSet("b")
	group.mu.Lock()
	group.states["b"].alive = true
	group.mu.Unlock()

	group.settleActive(proxies)
	require.Equal(t, "b", group.active)
}

func TestCascadeDialFailsOverWithinOneConnection(t *testing.T) {
	log := &dialLog{}
	group := newCascadeForTest(t, CascadeOption{},
		newCascadeFailingProxy("a", log),
		newCascadeSucceedingProxy("b", log),
		newCascadeFailingProxy("c", log),
	)

	conn, err := group.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Equal(t, []string{"a", "b"}, log.list(), "the failed candidate must be replaced inside the same dial")

	group.mu.RLock()
	require.True(t, group.states["a"].failedUntil.After(time.Now()))
	require.False(t, group.states["a"].alive)
	require.Equal(t, "b", group.active)
	group.mu.RUnlock()

	log.reset()
	_, err = group.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, log.list(), "the proven candidate must be dialed first")
}

func TestCascadeDialRespectsMaxAttempts(t *testing.T) {
	log := &dialLog{}
	group := newCascadeForTest(t, CascadeOption{MaxAttempts: intPtr(2)},
		newCascadeFailingProxy("a", log),
		newCascadeFailingProxy("b", log),
		newCascadeFailingProxy("c", log),
		newCascadeFailingProxy("d", log),
	)

	_, err := group.DialContext(context.Background(), &C.Metadata{})
	require.Error(t, err)
	require.Equal(t, []string{"a", "b"}, log.list())

	group.mu.RLock()
	_, probedC := group.states["c"]
	group.mu.RUnlock()
	require.False(t, probedC)
}

func TestCascadeDoesNotDemoteDirectFailures(t *testing.T) {
	log := &dialLog{}
	group := newCascadeForTest(t, CascadeOption{},
		newCascadeFakeProxy("a", C.Direct, func(context.Context, *C.Metadata) (C.Conn, error) {
			log.record("a")
			return nil, errors.New("dial failed")
		}, nil, 0),
		newCascadeFakeProxy("b", C.Compatible, func(context.Context, *C.Metadata) (C.Conn, error) {
			log.record("b")
			return nil, errors.New("dial failed")
		}, nil, 0),
	)

	_, err := group.DialContext(context.Background(), &C.Metadata{})
	require.Error(t, err)
	require.Equal(t, []string{"a", "b"}, log.list())

	group.mu.RLock()
	require.Empty(t, group.states)
	require.Empty(t, group.active)
	group.mu.RUnlock()
}

func TestCascadeCandidateOrderIgnoresRemovedHead(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{}, newCascadeTestProxy("a"), newCascadeTestProxy("b"))

	// the pinned and active names may point to members that were already
	// removed from the providers; they must not disturb the order
	group.mu.Lock()
	group.selected = "removed"
	group.active = "removed"
	group.lastActive = "removed"
	group.mu.Unlock()

	order := group.candidateOrder(group.GetProxies(false))
	require.Equal(t, []string{"a", "b"}, cascadeProxyNames(order))
}

func TestCascadeCandidateOrderDeduplicatesHead(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{}, newCascadeTestProxy("a"), newCascadeTestProxy("b"))

	group.mu.Lock()
	group.selected = "b"
	group.active = "b"
	group.lastActive = "b"
	group.mu.Unlock()

	order := group.candidateOrder(group.GetProxies(false))
	require.Equal(t, []string{"b", "a"}, cascadeProxyNames(order), "the same head member must be added once")
}

func TestCascadeCandidateOrderDeduplicatesMembers(t *testing.T) {
	a := newCascadeTestProxy("a")
	group := newCascadeForTest(t, CascadeOption{}, a, newCascadeTestProxy("b"))

	// overlapping providers may expose proxies with the same name; the order
	// must keep every candidate once so it is not dialed twice per connection
	proxies := append([]C.Proxy(nil), group.GetProxies(false)...)
	proxies = append(proxies, a)

	order := group.candidateOrder(proxies)
	require.Equal(t, []string{"a", "b"}, cascadeProxyNames(order))
}

func TestCascadeCheckWarmsLookahead(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{Lookahead: intPtr(1)},
		newCascadeFakeProxy("a", C.Shadowsocks, nil, errors.New("timeout"), 0),
		newCascadeFakeProxy("b", C.Shadowsocks, nil, nil, 120),
		newCascadeFakeProxy("c", C.Shadowsocks, nil, nil, 140),
		newCascadeFakeProxy("d", C.Shadowsocks, nil, nil, 160),
	)
	group.check()

	group.mu.RLock()
	defer group.mu.RUnlock()

	require.False(t, group.states["a"].alive)
	require.True(t, group.states["b"].alive)
	require.True(t, group.states["c"].alive)
	_, checkedD := group.states["d"]
	require.False(t, checkedD, "the check must stop once the window is warm")
	require.Equal(t, "b", group.active)
}

func TestCascadeCheckProbeBudget(t *testing.T) {
	proxies := make([]C.Proxy, 0, 6)
	for _, name := range []string{"a", "b", "c", "d", "e", "f"} {
		proxies = append(proxies, newCascadeFakeProxy(name, C.Shadowsocks, nil, errors.New("timeout"), 0))
	}

	// lookahead 0 keeps one candidate warm, the budget allows only two probes
	group := newCascadeForTest(t, CascadeOption{Lookahead: intPtr(0), FailedCooldown: intPtr(0)}, proxies...)
	group.check()

	group.mu.RLock()
	defer group.mu.RUnlock()

	require.Len(t, group.states, 2, "a dead member list must not be swept in full")
	require.Contains(t, group.states, "a")
	require.Contains(t, group.states, "b")
	require.Empty(t, group.active)
}

func TestCascadeMarshalJSON(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{}, newCascadeTestProxy("a"), newCascadeTestProxy("b"))
	group.ForceSet("b")

	data, err := group.MarshalJSON()
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(data, &payload))
	require.Equal(t, C.Cascade.String(), payload["type"])
	require.Equal(t, "b", payload["now"])
	require.Equal(t, "b", payload["fixed"])
	require.Equal(t, float64(2), payload["lookahead"])
	require.Equal(t, []any{"a", "b"}, payload["all"])
}
