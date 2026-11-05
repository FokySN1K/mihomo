package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
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

func boolPtr(v bool) *bool {
	return &v
}

// cascadeProbeProxy lets a test decide how a health check behaves, so a
// background recheck can be observed without touching the network.
type cascadeProbeProxy struct {
	*cascadeFakeProxy
	testFn func() (uint16, error)
}

func (p *cascadeProbeProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	return p.testFn()
}

// newCascadeUnreachableProxy fails both the dial and the health check, so a
// background probe cannot clear its demotion.
func newCascadeUnreachableProxy(name string) *cascadeFakeProxy {
	return newCascadeFakeProxy(name, C.Shadowsocks, nil, errors.New("timeout"), 0)
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
	require.True(t, group.preferFirst)
	require.Zero(t, group.attemptTimeout)
	require.Equal(t, defaultCascadeSwitchGrace, group.switchGrace)

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
			PreferFirst:    boolPtr(true),
			AttemptTimeout: intPtr(1500),
		}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	require.Equal(t, 4, group.lookahead)
	require.Equal(t, 7, group.maxAttempts)
	require.Equal(t, 15*time.Second, group.failedCooldown)
	require.True(t, group.preferFirst)
	require.Equal(t, 1500*time.Millisecond, group.attemptTimeout)
	require.Equal(t, defaultCascadeSwitchGrace, group.switchGrace)

	group, err = NewCascade(GroupCommonOption{Name: "cascade"},
		CascadeOption{SwitchGrace: intPtr(8)}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Equal(t, 8*time.Second, group.switchGrace)

	group, err = NewCascade(GroupCommonOption{Name: "cascade"},
		CascadeOption{SwitchGrace: intPtr(-1)}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Zero(t, group.switchGrace, "a negative switch-grace disables the grace window")
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

	// a pinned candidate jumps the queue, and in sticky mode the active one
	// stays right after it; prefer-first (the default) drops it back into the
	// rank walk instead, so the configured order decides
	group.mu.Lock()
	group.preferFirst = false
	group.mu.Unlock()
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
	// the sticky mode stays reachable through the option
	group := newCascadeForTest(t, CascadeOption{PreferFirst: boolPtr(false)}, newCascadeTestProxy("a"), newCascadeTestProxy("b"))
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

func TestCascadePreferFirstReturnsToFirstAlive(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{PreferFirst: boolPtr(true)},
		newCascadeTestProxy("a"), newCascadeTestProxy("b"))
	proxies := group.GetProxies(false)

	group.mu.Lock()
	group.states["a"] = &cascadeState{alive: true}
	group.states["b"] = &cascadeState{alive: true}
	group.active = "b"
	group.mu.Unlock()

	order := group.candidateOrder(proxies)
	require.Equal(t, []string{"a", "b"}, cascadeProxyNames(order),
		"prefer-first must not pin the fallback that took over")

	group.settleActive(proxies)
	group.mu.RLock()
	require.Equal(t, "a", group.active)
	group.mu.RUnlock()

	// an explicit choice still wins
	group.ForceSet("b")
	group.settleActive(proxies)
	group.mu.RLock()
	require.Equal(t, "b", group.active)
	group.mu.RUnlock()
}

func TestCascadeAttemptTimeoutCutsHangingDial(t *testing.T) {
	log := &dialLog{}
	hang := newCascadeFakeProxy("hang", C.Shadowsocks, func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		log.record("hang")
		<-ctx.Done()
		return nil, ctx.Err()
	}, errors.New("timeout"), 0)
	group := newCascadeForTest(t, CascadeOption{AttemptTimeout: intPtr(30), FailedCooldown: intPtr(30)},
		hang, newCascadeSucceedingProxy("ok", log))

	start := time.Now()
	conn, err := group.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, []string{"hang", "ok"}, log.list())

	group.mu.RLock()
	cut := *group.states["hang"]
	group.mu.RUnlock()
	require.False(t, cut.alive)
	require.Equal(t, 1, cut.timeoutCount)
	require.WithinDuration(t, time.Now().Add(cascadeTimeoutCooldown), cut.failedUntil, 2*time.Second,
		"a cut candidate is demoted softly, not for the full failed-cooldown")
}

func TestCascadeTimeoutCutEscalatesAfterRepeatedCuts(t *testing.T) {
	proxy := newCascadeUnreachableProxy("flaky")
	group := newCascadeForTest(t, CascadeOption{FailedCooldown: intPtr(30)}, proxy)

	group.onAttemptTimedOut(proxy)
	group.mu.RLock()
	first := *group.states["flaky"]
	group.mu.RUnlock()
	require.Equal(t, 1, first.timeoutCount)
	require.WithinDuration(t, time.Now().Add(cascadeTimeoutCooldown), first.failedUntil, 2*time.Second)

	// a cut outside the window starts the count over instead of accumulating
	group.mu.Lock()
	group.states["flaky"].timeoutAt = time.Now().Add(-cascadeTimeoutEscalateWindow - time.Second)
	group.mu.Unlock()
	group.onAttemptTimedOut(proxy)
	group.mu.RLock()
	second := *group.states["flaky"]
	group.mu.RUnlock()
	require.Equal(t, 1, second.timeoutCount, "cuts outside the window must not accumulate")

	for i := 0; i < cascadeTimeoutEscalateAfter; i++ {
		group.onAttemptTimedOut(proxy)
	}
	group.mu.RLock()
	escalated := *group.states["flaky"]
	group.mu.RUnlock()
	require.GreaterOrEqual(t, escalated.timeoutCount, cascadeTimeoutEscalateAfter)
	require.WithinDuration(t, time.Now().Add(group.failedCooldown), escalated.failedUntil, 2*time.Second,
		"a candidate cut over and over must take the full failed-cooldown")
}

func TestCascadeTimeoutCutRecheckRestoresCandidate(t *testing.T) {
	proxy := newCascadeTestProxy("slow")
	group := newCascadeForTest(t, CascadeOption{}, proxy)

	group.onAttemptTimedOut(proxy)

	require.Eventually(t, func() bool {
		group.mu.RLock()
		defer group.mu.RUnlock()
		state, ok := group.states["slow"]
		return ok && state.alive && state.failedUntil.IsZero()
	}, 2*time.Second, 10*time.Millisecond, "the background probe must clear the soft demotion")
}

func TestCascadeTimeoutCutRecheckThrottled(t *testing.T) {
	var probes atomic.Int32
	proxy := &cascadeProbeProxy{cascadeFakeProxy: newCascadeTestProxy("slow")}
	proxy.testFn = func() (uint16, error) {
		probes.Add(1)
		return 100, nil
	}
	group := newCascadeForTest(t, CascadeOption{}, proxy)

	group.onAttemptTimedOut(proxy)
	require.Eventually(t, func() bool { return probes.Load() == 1 }, time.Second, 5*time.Millisecond)

	group.onAttemptTimedOut(proxy)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), probes.Load(), "a second cut must not probe again inside the interval")
}

func TestCascadeAttemptContextSkipsNestedAttempts(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{AttemptTimeout: intPtr(30)}, newCascadeTestProxy("a"))
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctx, cancelAttempt := group.attemptContext(parent, newCascadeTestProxy("plain"))
	deadline, ok := ctx.Deadline()
	cancelAttempt()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(30*time.Millisecond), deadline, 25*time.Millisecond)

	// a nested pool or cascade retries its own members, so it keeps its budget
	pool := newPoolForTest(t, URLTestPoolOption{}, newCascadeTestProxy("p"))
	ctx, cancelAttempt = group.attemptContext(parent, adapter.NewProxy(pool))
	cancelAttempt()
	require.Equal(t, parent, ctx, "a nested pool must not be cut")

	nested := newCascadeForTest(t, CascadeOption{}, newCascadeTestProxy("n"))
	ctx, cancelAttempt = group.attemptContext(parent, adapter.NewProxy(nested))
	cancelAttempt()
	require.Equal(t, parent, ctx, "a nested cascade must not be cut")
}

func TestCascadeAttemptContextFollowsSelector(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{AttemptTimeout: intPtr(30)}, newCascadeTestProxy("a"))
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	inner := adapter.NewProxy(newPoolForTest(t, URLTestPoolOption{}, newCascadeTestProxy("p")))
	pd, err := provider.NewCompatibleProvider("selector", []C.Proxy{inner},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)
	selector, err := NewSelector(GroupCommonOption{Name: "selector"}, SelectorOption{DefaultSelected: "pool"},
		newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)

	ctx, cancelAttempt := group.attemptContext(parent, adapter.NewProxy(selector))
	cancelAttempt()
	require.Equal(t, parent, ctx, "a selector leading to a nested pool must not be cut")
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

func TestCascadeWrapsPoolAttemptTimeout(t *testing.T) {
	log := &dialLog{}
	pool := newPoolForTest(t, URLTestPoolOption{AttemptTimeout: intPtr(60)},
		newCascadeFakeProxy("dead", C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
			return nil, errors.New("dial failed")
		}, nil, 0),
		newCascadeFakeProxy("alive", C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
			log.record("alive")
			return &cascadeFakeConn{}, nil
		}, nil, 0),
	)

	group := newCascadeForTest(t, CascadeOption{}, adapter.NewProxy(pool))
	conn, err := group.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Equal(t, []string{"alive"}, log.list(), "the pool must replace its dead member inside the cascade attempt")
	require.Equal(t, "pool", group.Now(), "the cascade must keep the pool as the active candidate")
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

// TestCascadeCheckRecoversDemotedHead guards the case that kept the group on
// the fallback: with lookahead 0 the walk stops at the first alive candidate,
// so the demoted priority candidate is only reached by the recovery probe.
func TestCascadeCheckRecoversDemotedHead(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{Lookahead: intPtr(0)},
		newCascadeTestProxy("ru"), newCascadeTestProxy("pool"))

	group.mu.Lock()
	group.states["ru"] = &cascadeState{failedUntil: time.Now().Add(-time.Second)}
	group.states["pool"] = &cascadeState{alive: true}
	group.active = "pool"
	group.mu.Unlock()

	group.runCheck(context.Background(), false)

	group.mu.RLock()
	defer group.mu.RUnlock()
	require.True(t, group.states["ru"].alive, "the demoted head must be re-probed once its cooldown is over")
	require.Equal(t, "ru", group.active, "a recovered priority candidate must take over again")
}

// TestCascadeCheckRespectsCooldown guards the other half: a candidate still
// inside its cooldown is not probed by the periodic check, and the check arms a
// timer for the moment the cooldown ends instead of waiting a whole interval.
func TestCascadeCheckRespectsCooldown(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{Lookahead: intPtr(0)},
		newCascadeTestProxy("ru"), newCascadeTestProxy("pool"))

	group.mu.Lock()
	group.states["ru"] = &cascadeState{failedUntil: time.Now().Add(time.Minute)}
	group.states["pool"] = &cascadeState{alive: true}
	group.active = "pool"
	group.mu.Unlock()

	group.runCheck(context.Background(), false)

	group.mu.RLock()
	alive := group.states["ru"].alive
	active := group.active
	group.mu.RUnlock()
	require.False(t, alive, "a cooling candidate must stay out of the periodic check")
	require.Equal(t, "pool", active)
	require.True(t, group.recoveryPending.Load(), "the check must arm a timer for the cooldown expiry")
}

// TestCascadeSelfTestForcesPriorityCandidate guards the explicit test a host
// runs: it must re-probe a demoted priority candidate even inside its cooldown,
// so testing the group can move it back.
func TestCascadeSelfTestForcesPriorityCandidate(t *testing.T) {
	group := newCascadeForTest(t, CascadeOption{Lookahead: intPtr(0)},
		newCascadeTestProxy("ru"), newCascadeTestProxy("pool"))

	group.mu.Lock()
	group.states["ru"] = &cascadeState{failedUntil: time.Now().Add(time.Minute)}
	group.states["pool"] = &cascadeState{alive: true}
	group.active = "pool"
	group.mu.Unlock()

	results, err := group.URLTest(context.Background(), group.testUrl, nil)
	require.NoError(t, err)
	require.Contains(t, results, "ru", "an explicit test must measure the demoted candidate too")

	group.mu.RLock()
	defer group.mu.RUnlock()
	require.Equal(t, "ru", group.active)
}

// TestCascadeProbesGroupCandidateAsGroup guards the reason the cascade kept
// preferring the fallback: a single dial through a load-balancing candidate
// exercises one member, and one dead member must not demote the candidate.
func TestCascadeProbesGroupCandidateAsGroup(t *testing.T) {
	dead := newCascadeUnreachableProxy("ru-dead")
	alive := newCascadeTestProxy("ru-alive")
	pd, err := provider.NewCompatibleProvider("ru", []C.Proxy{dead, alive},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	balance, err := NewLoadBalance(GroupCommonOption{Name: "ru", URL: poolTestUrl},
		LoadBalanceOption{Strategy: "round-robin"}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)

	group := newCascadeForTest(t, CascadeOption{Lookahead: intPtr(0)}, adapter.NewProxy(balance))
	proxies := group.GetProxies(false)
	require.Len(t, proxies, 1)

	delay, err := group.probeCandidate(context.Background(), proxies[0])
	require.NoError(t, err)
	require.Equal(t, uint16(100), delay,
		"a group candidate is alive while any of its members answers")
}

// TestCascadeProbesPoolMembersOnly guards the cost of a check: a pool candidate
// is measured over the members the pool holds, not over every candidate of its
// providers, and the measurement does not rebuild the pool from a sweep.
func TestCascadeProbesPoolMembersOnly(t *testing.T) {
	var probes atomic.Int32
	newCounting := func(name string) C.Proxy {
		proxy := &cascadeProbeProxy{cascadeFakeProxy: newCascadeTestProxy(name)}
		proxy.testFn = func() (uint16, error) {
			probes.Add(1)
			return 100, nil
		}
		return proxy
	}

	pool := newPoolForTest(t, URLTestPoolOption{},
		newCounting("a"), newCounting("b"), newCounting("outside"))
	pool.pool = []string{"a", "b"}

	group := newCascadeForTest(t, CascadeOption{}, adapter.NewProxy(pool))
	proxies := group.GetProxies(false)
	require.Len(t, proxies, 1)

	delay, err := group.probeCandidate(context.Background(), proxies[0])
	require.NoError(t, err)
	require.Equal(t, uint16(100), delay)
	require.Equal(t, int32(2), probes.Load(),
		"a pool candidate must be measured over its pool, not over every member")
}

// TestCascadePoolKeepsTheConnection guards the dial path: one failed dial
// through a pool that still has a working member must not hand the connection
// to the next candidate, because the pool moves to that member by itself.
func TestCascadePoolKeepsTheConnection(t *testing.T) {
	log := &dialLog{}
	pool := newPoolForTest(t, URLTestPoolOption{},
		newPoolFailingProxy("dead", log),
		newPoolSucceedingProxy("alive", log),
	)
	group := newCascadeForTest(t, CascadeOption{}, adapter.NewProxy(pool),
		newCascadeSucceedingProxy("fallback", log))

	conn, err := group.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Equal(t, []string{"dead", "alive"}, log.list(),
		"the pool must keep the connection and move to its next member")

	group.mu.RLock()
	state := group.states["pool"]
	group.mu.RUnlock()
	require.NotNil(t, state)
	require.True(t, state.alive, "a pool that still has a working member keeps its place")
	require.True(t, state.failedUntil.IsZero())
}

// TestCascadePoolDemotedAfterSwitchGrace guards the window: a pool that keeps
// failing after the grace has expired must lose its place, so the group finally
// switches to the next candidate.
func TestCascadePoolDemotedAfterSwitchGrace(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{},
		newPoolDeadProxy("a"), newPoolDeadProxy("b"))
	proxy := adapter.NewProxy(pool)
	group := newCascadeForTest(t, CascadeOption{}, newCascadeTestProxy("ru"))
	group.switchGrace = 30 * time.Millisecond

	require.True(t, group.onAttemptFailed(proxy, errors.New("dial failed")),
		"the first failure opens the window and keeps the candidate")
	group.mu.RLock()
	state := group.states["pool"]
	group.mu.RUnlock()
	require.True(t, state.failedUntil.IsZero(), "a kept candidate must not enter a cooldown")

	time.Sleep(50 * time.Millisecond)

	require.False(t, group.onAttemptFailed(proxy, errors.New("dial failed")),
		"the failure after the window demotes the candidate")
	group.mu.RLock()
	state = group.states["pool"]
	group.mu.RUnlock()
	require.False(t, state.alive)
	require.WithinDuration(t, time.Now().Add(group.failedCooldown), state.failedUntil, 2*time.Second)
}

// TestCascadePoolWithoutAliveMemberIsDemoted guards the "no nodes available"
// half: a pool that reports nothing alive is cut at once, without waiting for
// the window.
func TestCascadePoolWithoutAliveMemberIsDemoted(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	// a finished check found nothing alive
	pool.hasFullCheck.Store(true)

	proxy := adapter.NewProxy(pool)
	group := newCascadeForTest(t, CascadeOption{}, proxy)

	require.False(t, group.onAttemptFailed(proxy, errors.New("dial failed")))
	group.mu.RLock()
	state := group.states["pool"]
	group.mu.RUnlock()
	require.NotNil(t, state)
	require.False(t, state.alive)
	require.WithinDuration(t, time.Now().Add(group.failedCooldown), state.failedUntil, 2*time.Second,
		"a pool with nothing left takes the full cooldown")
}
