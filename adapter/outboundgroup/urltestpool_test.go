package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

const poolTestUrl = "https://cp.cloudflare.com/generate_204"

func newPoolEmptyFallback() C.Proxy {
	return adapter.NewProxy(outbound.NewDirect())
}

type poolFakeAdapter struct {
	*outbound.Base
	dialFn func(ctx context.Context, metadata *C.Metadata) (C.Conn, error)
}

func (f *poolFakeAdapter) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	return f.dialFn(ctx, metadata)
}

// newPoolDeadProxy returns a proxy whose dial always fails, so URLTest marks it
// dead without touching the network.
func newPoolDeadProxy(name string) C.Proxy {
	base := outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Shadowsocks, UDP: true})
	return adapter.NewProxy(&poolFakeAdapter{Base: base, dialFn: func(context.Context, *C.Metadata) (C.Conn, error) {
		return nil, errors.New("dial failed")
	}})
}

func TestURLTestPoolDefaults(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Interval: 300, Lazy: true}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	require.Equal(t, C.URLTestPool, pool.Type())
	require.Equal(t, 10, pool.poolSize)
	require.Equal(t, 300*time.Second, pool.interval)
	require.Equal(t, 1800*time.Second, pool.fullInterval)
	require.Equal(t, 3, pool.degradedChecks)
	require.Equal(t, defaultURLTestTolerance, pool.tolerance)
	require.True(t, pool.lazy)
	require.Equal(t, C.DefaultTestURL, pool.testUrl)
}

func TestURLTestPoolIntervalDefaults(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	require.Equal(t, 300*time.Second, pool.interval)
	require.Equal(t, 1800*time.Second, pool.fullInterval)
}

func TestURLTestPoolValidatesFullInterval(t *testing.T) {
	_, err := NewURLTestPool(GroupCommonOption{Name: "pool", Interval: 300},
		URLTestPoolOption{FullInterval: 60}, newPoolEmptyFallback(), nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "full-interval")
}

func TestURLTestPoolRejectsNestedGroup(t *testing.T) {
	emptyFallback := newPoolEmptyFallback()
	inner, err := NewURLTest(GroupCommonOption{Name: "inner", URL: poolTestUrl}, URLTestOption{}, emptyFallback, nil)
	require.NoError(t, err)

	groupProxy := adapter.NewProxy(inner)
	pd, err := provider.NewCompatibleProvider("pool", []C.Proxy{groupProxy},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	_, err = NewURLTestPool(GroupCommonOption{Name: "pool"},
		URLTestPoolOption{}, emptyFallback, []P.ProxyProvider{pd})
	require.Error(t, err)
	require.ErrorContains(t, err, "can't contain proxy group")
}

func TestURLTestPoolDegradedChecks(t *testing.T) {
	checks := 2
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{DegradedChecks: &checks}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Equal(t, 2, pool.degradedChecks)

	pool.pool = []string{"a", "b", "c"}

	require.False(t, pool.shouldRunFullCheck(3, 3)) // a full pool resets the counter
	require.False(t, pool.shouldRunFullCheck(2, 3)) // first underfilled quick check
	require.True(t, pool.shouldRunFullCheck(2, 3))  // second one triggers a full check
	require.False(t, pool.shouldRunFullCheck(2, 3)) // the counter has been reset
}

func TestURLTestPoolDegradedChecksDisabled(t *testing.T) {
	checks := 0
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{DegradedChecks: &checks}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Zero(t, pool.degradedChecks)

	pool.pool = []string{"a", "b"}
	require.False(t, pool.shouldRunFullCheck(1, 2))
	require.False(t, pool.shouldRunFullCheck(1, 2))
}

func TestURLTestPoolDegradedChecksSmallPool(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	// fewer proxies than pool-size: a fully alive small pool is not degraded
	pool.pool = []string{"a", "b"}
	target := pool.poolTarget(2)
	require.Equal(t, 2, target)
	require.False(t, pool.shouldRunFullCheck(2, target))
	require.False(t, pool.shouldRunFullCheck(2, target))
}

func TestURLTestPoolProxiesReady(t *testing.T) {
	emptyFallback := newPoolEmptyFallback()
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, emptyFallback, nil)
	require.NoError(t, err)

	require.False(t, pool.proxiesReady([]C.Proxy{emptyFallback}))
	require.True(t, pool.proxiesReady([]C.Proxy{emptyFallback, adapter.NewProxy(outbound.NewDirect())}))
}

// mutableProvider is a provider whose list arrives after the group was built,
// the way an embedded core loads it after applying the config.
type mutableProvider struct {
	P.ProxyProvider
	name string

	mu      sync.RWMutex
	proxies []C.Proxy
	version uint32
}

func (m *mutableProvider) Name() string { return m.name }

func (m *mutableProvider) Proxies() []C.Proxy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.proxies
}

func (m *mutableProvider) Version() uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version
}

func (m *mutableProvider) set(proxies ...C.Proxy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.proxies = proxies
	m.version++
}

func TestURLTestPoolWaitProvidersReadyWakesOnLoad(t *testing.T) {
	pd := &mutableProvider{name: "pd"}
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{},
		newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)

	loaded := make(chan struct{}, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		pd.set(namedProxy("pd-hk"))
		loaded <- struct{}{}
	}()

	start := time.Now()
	require.True(t, pool.waitProvidersReady(5*time.Second, loaded))
	require.Less(t, time.Since(start), urlTestPoolPreWarmRecheck,
		"the wait must end on the load signal, not on the recheck tick")
}

func TestURLTestPoolWaitProvidersReadyGivesUp(t *testing.T) {
	pd := &mutableProvider{name: "pd"}
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{},
		newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)

	start := time.Now()
	require.False(t, pool.waitProvidersReady(50*time.Millisecond, make(chan struct{})))
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}

func TestURLTestPoolNoRecoveryCheckWithoutEmptyPool(t *testing.T) {
	checks := 0
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{DegradedChecks: &checks}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	pool.pool = []string{"a"}
	pool.noteDialSucceeded(newPoolEmptyFallback())
	require.False(t, pool.testing.Load())
}

func TestURLTestPoolExhaustionCheckThrottled(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	now := time.Now()
	pool.pool = []string{"a"}
	pool.lastFull.Store(now)
	require.False(t, pool.shouldRequestFullCheckOnExhaustion(now),
		"a full check that just found the pool alive must not be repeated")
	require.True(t, pool.shouldRequestFullCheckOnExhaustion(now.Add(pool.interval+time.Second)))

	pool.pool = nil
	pool.lastEmptyFull = now
	require.False(t, pool.shouldRequestFullCheckOnExhaustion(now),
		"an empty pool keeps its own throttle")
	require.True(t, pool.shouldRequestFullCheckOnExhaustion(now.Add(pool.interval+time.Second)))
}

func TestURLTestPoolSortedProxyNames(t *testing.T) {
	results := map[string]uint16{
		"slow":   300,
		"fast-b": 100,
		"fast-a": 100,
	}

	require.Equal(t, []string{"fast-a", "fast-b", "slow"}, sortedProxyNames(results))
}

func TestURLTestPoolKeepsLastPoolAfterEmptyCheck(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	pool.applyResults(map[string]uint16{"a": 20, "b": 10})
	require.Equal(t, []string{"b", "a"}, pool.lastPool)

	pool.applyResults(map[string]uint16{})
	require.Empty(t, pool.pool)
	require.Equal(t, []string{"b", "a"}, pool.lastPool, "the last pool must survive an empty check")
}

func TestURLTestPoolRemembersLastGood(t *testing.T) {
	proxy := newPoolDeadProxy("a")
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	pool.pool = []string{"a"}
	pool.lastFull.Store(time.Now())
	pool.noteDialSucceeded(proxy)

	require.Equal(t, "a", pool.lastGood)
	require.False(t, pool.testing.Load())
}

func TestURLTestPoolFallsBackToLastGood(t *testing.T) {
	a := newPoolDeadProxy("a")
	b := newPoolDeadProxy("b")
	pd, err := provider.NewCompatibleProvider("pool", []C.Proxy{a, b},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"
	_, _ = a.URLTest(context.Background(), pool.testUrl, pool.expectedRanges)
	_, _ = b.URLTest(context.Background(), pool.testUrl, pool.expectedRanges)

	pool.pool = []string{"a", "b"}
	pool.lastPool = []string{"a", "b"}
	pool.lastGood = "b"
	pool.hasFullCheck.Store(true)
	pool.lastFull.Store(time.Now())
	pool.lastQuick.Store(time.Now())

	proxy, recovery := pool.fastForDial(false)
	require.True(t, recovery)
	require.Equal(t, "b", proxy.Name(), "an exhausted pool must dial the last known good proxy")
}

func TestURLTestPoolRecoverySkipsWithoutTime(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	pool.lastPool = []string{"a"}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	require.Nil(t, pool.recoverAfterFailure(ctx))

	pool.lastRecovery.Store(time.Now())
	require.Nil(t, pool.recoverAfterFailure(context.Background()), "the recovery test must be throttled")
}

// newExhaustedPoolForTest builds a pool with nothing usable left whose last
// pool still holds the given members, the state a dial of an outage sees: the
// names the pool lists no longer resolve, which pickFast treats as stale.
func newExhaustedPoolForTest(t *testing.T, proxies ...C.Proxy) *URLTestPool {
	t.Helper()

	pool := newPoolForTest(t, URLTestPoolOption{}, proxies...)
	pool.mu.Lock()
	pool.pool = []string{"gone"}
	pool.lastPool = cascadeProxyNames(proxies)
	pool.mu.Unlock()
	return pool
}

// A probe the caller cut says nothing about the member: PoolCheck must report
// the cancellation instead of wiping the pool the parent dials through.
func TestURLTestPoolPoolCheckRespectsCallerDeadline(t *testing.T) {
	held := newPoolRelease()
	t.Cleanup(held.close)

	blocked := newPoolControlledProxy("blocked", nil, poolProbeHeld(held))
	pool := newPoolForTest(t, URLTestPoolOption{}, blocked)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	results, err := pool.PoolCheck(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, results)
	require.Equal(t, []string{"blocked"}, poolNames(pool),
		"a probe the caller cut must not be taken as a dead member")
}

// PoolCheck publishes what it measured even when the caller cuts the rest, and
// keeps the members it did not reach instead of dropping them.
func TestURLTestPoolPoolCheckPublishesWhatItMeasured(t *testing.T) {
	held := newPoolRelease()
	t.Cleanup(held.close)

	fast := newPoolControlledProxy("fast", nil, poolProbeOk(10))
	slow := newPoolControlledProxy("slow", nil, poolProbeHeld(held))
	pool := newPoolForTest(t, URLTestPoolOption{}, fast, slow)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	results, err := pool.PoolCheck(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]uint16{"fast": 10}, results)
	require.Equal(t, []string{"fast", "slow"}, poolNames(pool),
		"the measured member goes first and the member the check did not reach stays in the pool")
}

// Every dial of one outage joins the same emergency test instead of starting a
// probe of its own.
func TestURLTestPoolRecoveryJoinsOneTest(t *testing.T) {
	var probes atomic.Int32
	measured := newPoolControlledProxy("measured", nil, func(context.Context) (uint16, error) {
		probes.Add(1)
		time.Sleep(200 * time.Millisecond)
		return 10, nil
	})

	pool := newExhaustedPoolForTest(t, measured)

	found := make(chan C.Proxy, 3)
	start := make(chan struct{})
	for i := 0; i < cap(found); i++ {
		go func() {
			<-start
			found <- pool.recoverAfterFailure(context.Background())
		}()
	}
	close(start)

	for i := 0; i < cap(found); i++ {
		require.NotNil(t, <-found, "every dial of the outage must get the measured member")
	}
	require.Equal(t, int32(1), probes.Load(),
		"dials of one outage must join the running test instead of probing on their own")
}

// A second test within the cooldown must not start: an ongoing outage cannot
// turn every connection into a burst of probes.
func TestURLTestPoolRecoveryThrottled(t *testing.T) {
	var probes atomic.Int32
	measured := newPoolControlledProxy("measured", nil, func(context.Context) (uint16, error) {
		probes.Add(1)
		return 10, nil
	})

	pool := newExhaustedPoolForTest(t, measured)
	require.NotNil(t, pool.recoverAfterFailure(context.Background()))

	require.Eventually(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		return pool.measuring == nil
	}, time.Second, time.Millisecond, "the test must end before the cooldown is checked")

	require.Nil(t, pool.recoverAfterFailure(context.Background()),
		"a second test within the cooldown must not start")
	require.Equal(t, int32(1), probes.Load())
}

// A dial whose budget is already inside the retry reserve must not spend it
// waiting for the emergency test.
func TestURLTestPoolRecoveryKeepsTheRetryReserve(t *testing.T) {
	held := newPoolRelease()
	t.Cleanup(held.close)

	measured := newPoolControlledProxy("measured", nil, poolProbeHeld(held))
	pool := newExhaustedPoolForTest(t, measured)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	start := time.Now()
	require.Nil(t, pool.recoverAfterFailure(ctx))
	require.Less(t, time.Since(start), 200*time.Millisecond,
		"a dial with no budget left for a retry must not wait for the emergency test")
}

// The dial of an exhausted pool leaves on the member the emergency test found
// instead of paying for a blind last-good dial first.
func TestURLTestPoolExhaustedDialLeavesOnMeasuredMember(t *testing.T) {
	log := &dialLog{}
	held := newPoolRelease()
	t.Cleanup(held.close)

	measured := newPoolControlledProxy("measured", func(context.Context, *C.Metadata) (C.Conn, error) {
		log.record("measured")
		return &cascadeFakeConn{}, nil
	}, poolProbeOk(10))
	blind := newPoolControlledProxy("blind", func(context.Context, *C.Metadata) (C.Conn, error) {
		log.record("blind")
		return &cascadeFakeConn{}, nil
	}, poolProbeHeld(held))

	pool := newExhaustedPoolForTest(t, measured, blind)

	conn, err := pool.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Equal(t, []string{"measured"}, log.list(),
		"an exhausted dial must leave on the member the emergency test measured")
}

// Only a sweep that reached every candidate is a verdict about the pool.
func TestURLTestPoolRecoveryMarksOnlyFullSweeps(t *testing.T) {
	pool := &URLTestPool{poolSize: 10, suspicious: map[string]time.Time{}}

	pool.applyRecovery(nil, map[string]struct{}{"a": {}}, 2)
	require.False(t, pool.hasFullCheck.Load(),
		"a test that did not reach every candidate is not a verdict about the pool")

	pool.applyRecovery(nil, map[string]struct{}{"a": {}, "b": {}}, 2)
	require.True(t, pool.hasFullCheck.Load(),
		"a test that reached every candidate and found nothing tells a parent the pool is empty")
}

func TestURLTestPoolTracksOnlyDialFailures(t *testing.T) {
	errDial := errors.New("dial failed")

	require.False(t, shouldTrackDialFailure(C.Direct, errDial))
	require.False(t, shouldTrackDialFailure(C.Compatible, errDial))
	require.False(t, shouldTrackDialFailure(C.Reject, errDial))
	require.False(t, shouldTrackDialFailure(C.RejectDrop, errDial))
	require.False(t, shouldTrackDialFailure(C.Shadowsocks, C.ErrNotSupport))

	require.True(t, shouldTrackDialFailure(C.Shadowsocks, errDial))
	require.True(t, shouldTrackDialFailure(C.URLTestPool, errDial))
}

func TestURLTestPoolAliveMember(t *testing.T) {
	fresh, err := NewURLTestPool(GroupCommonOption{Name: "fresh"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	alive, known := fresh.AliveMember()
	require.False(t, alive)
	require.False(t, known, "a pool that has not been measured yet must not be cut on its first failure")

	dead := newPoolDeadProxy("dead")
	pool := newPoolForTest(t, URLTestPoolOption{}, newCascadeTestProxy("alive"), dead)
	_, _ = dead.URLTest(context.Background(), pool.testUrl, pool.expectedRanges)

	alive, known = pool.AliveMember()
	require.True(t, alive, "a pool with a working member keeps its parent on it")
	require.True(t, known)

	pool.markSuspicious("alive")
	alive, known = pool.AliveMember()
	require.False(t, alive, "a pool whose every member failed has nothing left")
	require.True(t, known)

	pool.mu.Lock()
	pool.pool = nil
	pool.mu.Unlock()
	alive, known = pool.AliveMember()
	require.False(t, alive)
	require.True(t, known)
}

func TestURLTestPoolProxiesByNames(t *testing.T) {
	a := newPoolDeadProxy("a")
	b := newPoolDeadProxy("b")
	c := newPoolDeadProxy("c")

	resolved, complete := proxiesByNames([]C.Proxy{a, b, c}, []string{"c", "a"})
	require.True(t, complete)
	require.Equal(t, []string{"c", "a"}, cascadeProxyNames(resolved), "the requested order must be preserved")

	// a removed member makes the resolution incomplete but keeps the rest
	resolved, complete = proxiesByNames([]C.Proxy{a, c}, []string{"b", "c"})
	require.False(t, complete)
	require.Equal(t, []string{"c"}, cascadeProxyNames(resolved))

	resolved, complete = proxiesByNames([]C.Proxy{a}, nil)
	require.True(t, complete)
	require.Empty(t, resolved)
}

// newPoolForTest builds a pool whose scheduler is frozen, so unit tests never
// race with an automatically started health check.
func newPoolForTest(t *testing.T, opt URLTestPoolOption, proxies ...C.Proxy) *URLTestPool {
	t.Helper()

	pd, err := provider.NewCompatibleProvider("pool", proxies,
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Interval: 300},
		opt, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"
	pool.pool = cascadeProxyNames(proxies)
	pool.lastPool = append([]string(nil), pool.pool...)
	pool.hasFullCheck.Store(true)
	pool.lastFull.Store(time.Now())
	pool.lastQuick.Store(time.Now())
	return pool
}

func newPoolFailingProxy(name string, log *dialLog) *cascadeFakeProxy {
	return newCascadeFakeProxy(name, C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
		log.record(name)
		return nil, errors.New("dial failed")
	}, nil, 0)
}

func newPoolSucceedingProxy(name string, log *dialLog) *cascadeFakeProxy {
	return newCascadeFakeProxy(name, C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
		log.record(name)
		return &cascadeFakeConn{}, nil
	}, nil, 0)
}

// poolRelease unblocks the probes a test holds, so a measurement that a test
// left running ends with the test instead of waiting for its own timeout.
type poolRelease struct {
	once sync.Once
	ch   chan struct{}
}

func newPoolRelease() *poolRelease {
	return &poolRelease{ch: make(chan struct{})}
}

func (r *poolRelease) close() {
	r.once.Do(func() { close(r.ch) })
}

// poolProbeOk answers a health check with a fixed delay.
func poolProbeOk(delay uint16) func(context.Context) (uint16, error) {
	return func(context.Context) (uint16, error) { return delay, nil }
}

// poolProbeHeld blocks a health check the way a member that does not answer
// behaves: it ends only when the test releases it or the probe is cut.
func poolProbeHeld(held *poolRelease) func(context.Context) (uint16, error) {
	return func(ctx context.Context) (uint16, error) {
		select {
		case <-held.ch:
			return 0, errors.New("probe released")
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// poolControlledProxy is a member whose health check, dial and packet flow a
// test decides, so the dial path is exercised without touching the network.
type poolControlledProxy struct {
	*cascadeFakeProxy

	mu       sync.Mutex
	udp      bool
	probe    func(context.Context) (uint16, error)
	packetFn func() error
}

// newPoolControlledProxy builds a member that dials successfully unless the
// test gives it a dial of its own.
func newPoolControlledProxy(name string, dialFn func(context.Context, *C.Metadata) (C.Conn, error), probe func(context.Context) (uint16, error)) *poolControlledProxy {
	if dialFn == nil {
		dialFn = func(context.Context, *C.Metadata) (C.Conn, error) {
			return &cascadeFakeConn{}, nil
		}
	}
	return &poolControlledProxy{
		cascadeFakeProxy: newCascadeFakeProxy(name, C.Shadowsocks, dialFn, nil, 0),
		udp:              true,
		probe:            probe,
		packetFn:         func() error { return nil },
	}
}

func (p *poolControlledProxy) URLTest(ctx context.Context, _ string, _ utils.IntRanges[uint16]) (uint16, error) {
	return p.probe(ctx)
}

func (p *poolControlledProxy) SupportUDP() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.udp
}

func (p *poolControlledProxy) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	if err := p.packetFn(); err != nil {
		return nil, err
	}
	return &poolFakePacketConn{}, nil
}

// poolFakePacketConn satisfies C.PacketConn for a packet flow that is only
// recorded, never written to.
type poolFakePacketConn struct {
	N.EnhancePacketConn
}

func (c *poolFakePacketConn) Chains() C.Chain                               { return nil }
func (c *poolFakePacketConn) ProviderChains() C.Chain                       { return nil }
func (c *poolFakePacketConn) AppendToChains(C.ProxyAdapter)                 {}
func (c *poolFakePacketConn) RemoteDestination() string                     { return "" }
func (c *poolFakePacketConn) ResolveUDP(context.Context, *C.Metadata) error { return nil }

func TestURLTestPoolAttemptTimeoutTriesNextMember(t *testing.T) {
	log := &dialLog{}
	pool := newPoolForTest(t, URLTestPoolOption{AttemptTimeout: intPtr(1000)},
		newPoolFailingProxy("a", log),
		newPoolSucceedingProxy("b", log),
	)

	conn, err := pool.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Equal(t, []string{"a", "b"}, log.list())
	require.Equal(t, "b", pool.Now(), "the member that proved connectivity must be preferred")
}

func TestURLTestPoolAttemptTimeoutCutsHangingMember(t *testing.T) {
	a := newCascadeFakeProxy("a", C.Shadowsocks, func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil, 0)
	log := &dialLog{}
	pool := newPoolForTest(t, URLTestPoolOption{AttemptTimeout: intPtr(60)},
		a,
		newPoolSucceedingProxy("b", log),
	)

	start := time.Now()
	conn, err := pool.DialContext(context.Background(), &C.Metadata{})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Equal(t, []string{"b"}, log.list())
	require.GreaterOrEqual(t, elapsed, 60*time.Millisecond)
	require.Less(t, elapsed, 2*time.Second)
}

func TestURLTestPoolKeepsSingleDialWithoutAttemptTimeout(t *testing.T) {
	log := &dialLog{}
	pool := newPoolForTest(t, URLTestPoolOption{},
		newPoolFailingProxy("a", log),
		newPoolSucceedingProxy("b", log),
	)

	_, err := pool.DialContext(context.Background(), &C.Metadata{})
	require.Error(t, err)
	require.Equal(t, []string{"a"}, log.list(), "without attempt-timeout the pool keeps one dial per call")
}

func TestURLTestPoolAttemptTimeoutRunsFullCheckOnExhaustion(t *testing.T) {
	log := &dialLog{}
	brokenTest := errors.New("test failed")
	failing := func(name string) *cascadeFakeProxy {
		return newCascadeFakeProxy(name, C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
			log.record(name)
			return nil, errors.New("dial failed")
		}, brokenTest, 0)
	}
	pool := newPoolForTest(t, URLTestPoolOption{AttemptTimeout: intPtr(1000)}, failing("a"), failing("b"))

	_, err := pool.DialContext(context.Background(), &C.Metadata{})
	require.Error(t, err)
	require.Equal(t, []string{"a", "b"}, log.list())

	time.Sleep(100 * time.Millisecond)
	pool.mu.RLock()
	empty := pool.lastEmptyFull.IsZero()
	pool.mu.RUnlock()
	require.True(t, empty, "a recent full check must throttle exhaustion rechecks")

	// Once the full check is stale, exhaustion may probe the whole pool again.
	pool.lastFull.Store(time.Now().Add(-pool.interval - time.Second))
	pool.markSuspicious("a")

	require.Eventually(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		return !pool.lastEmptyFull.IsZero()
	}, 2*time.Second, 10*time.Millisecond, "a stale pool must trigger a full check")
}

func TestURLTestPoolAttemptTimeoutOption(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"},
		URLTestPoolOption{AttemptTimeout: intPtr(750)}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Equal(t, 750*time.Millisecond, pool.attemptTimeout)

	pool, err = NewURLTestPool(GroupCommonOption{Name: "pool"},
		URLTestPoolOption{AttemptTimeout: intPtr(-1)}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Zero(t, pool.attemptTimeout, "a negative attempt-timeout disables the bound")

	pool, err = NewURLTestPool(GroupCommonOption{Name: "pool"},
		URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Zero(t, pool.attemptTimeout)
}

func TestURLTestPoolToleranceOption(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Equal(t, defaultURLTestTolerance, pool.tolerance, "tolerance defaults to 150 ms")

	zero := uint16(0)
	pool, err = NewURLTestPool(GroupCommonOption{Name: "pool"},
		URLTestPoolOption{Tolerance: &zero}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Zero(t, pool.tolerance, "tolerance 0 disables the hysteresis")
}

func TestURLTestPoolProxiesLockedKeepsPoolOrder(t *testing.T) {
	a := newPoolDeadProxy("a")
	b := newPoolDeadProxy("b")
	c := newPoolDeadProxy("c")

	pool := &URLTestPool{pool: []string{"c", "a", "removed"}}
	require.Equal(t, []string{"c", "a"}, cascadeProxyNames(pool.poolProxiesLocked([]C.Proxy{a, b, c})))
}

func TestURLTestPoolFirstResultWaitFollowsTimeout(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Equal(t, defaultURLTestPoolFirstResultWait, pool.firstResultWait,
		"the default probe timeout is longer than the cap")

	pool, err = NewURLTestPool(GroupCommonOption{Name: "pool", TestTimeout: 800}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Equal(t, 800*time.Millisecond, pool.firstResultWait,
		"a result cannot be published before the probe timeout expires")
}

// newColdPoolForTest builds a pool that never ran a check, so a dial starts one.
func newColdPoolForTest(t *testing.T, opt URLTestPoolOption, proxies ...C.Proxy) *URLTestPool {
	t.Helper()

	pd, err := provider.NewCompatibleProvider("pool", proxies,
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Interval: 300},
		opt, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"
	return pool
}

// poolProbeProxy lets a test decide how a health check behaves, including
// blocking it, without touching the network.
type poolProbeProxy struct {
	*cascadeFakeProxy
	testFn func() (uint16, error)
}

func (p *poolProbeProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	return p.testFn()
}

func poolNames(pool *URLTestPool) []string {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return append([]string(nil), pool.pool...)
}

func poolLastNames(pool *URLTestPool) []string {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return append([]string(nil), pool.lastPool...)
}

// TestURLTestPoolPublishesEachResult guards the progressive publish: the first
// measured member must become the pool while the slow probes are still blocked,
// so a cold start routes by a real delay almost immediately.
func TestURLTestPoolPublishesEachResult(t *testing.T) {
	release := make(chan struct{})
	log := &dialLog{}

	fast := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy("fast", log)}
	fast.testFn = func() (uint16, error) { return 20, nil }
	proxies := []C.Proxy{fast}
	for i := 0; i < 2; i++ {
		slow := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy(fmt.Sprintf("slow-%d", i), log)}
		slow.testFn = func() (uint16, error) {
			<-release
			return 300, nil
		}
		proxies = append(proxies, slow)
	}

	pool := newColdPoolForTest(t, URLTestPoolOption{}, proxies...)

	done := make(chan struct{})
	go func() {
		pool.fullCheck()
		close(done)
	}()

	require.Eventually(t, func() bool { return len(poolNames(pool)) == 1 }, 2*time.Second, 5*time.Millisecond,
		"the first measured member must be published while the slow probes are blocked")
	require.Equal(t, []string{"fast"}, poolNames(pool))
	require.Empty(t, poolLastNames(pool), "a partial result must not replace the recovery set")
	select {
	case <-done:
		t.Fatal("the full check must still be running while the slow probes are blocked")
	default:
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the full check did not finish")
	}
	require.Equal(t, []string{"fast", "slow-0", "slow-1"}, poolNames(pool))
	require.Len(t, poolLastNames(pool), 3, "the final result must update the recovery set")
}

// TestURLTestPoolPartialPublishKeepsUnmeasured guards the pool a dial can choose
// from: a check in progress must not drop the members it has not re-measured
// yet, a member measured and failed must leave at once, and the final result
// may evict whatever the check did not reach.
func TestURLTestPoolPartialPublishKeepsUnmeasured(t *testing.T) {
	pool := &URLTestPool{poolSize: 3, pool: []string{"a", "b", "c"}}

	pool.publishPool(map[string]uint16{"b": 20}, nil, false)
	require.Equal(t, []string{"b", "a", "c"}, poolNames(pool),
		"the measured member goes first, the unmeasured ones stay behind it")

	pool.publishPool(map[string]uint16{"b": 20}, map[string]struct{}{"b": {}, "a": {}}, false)
	require.Equal(t, []string{"b", "c"}, poolNames(pool),
		"a member measured and failed leaves the pool, only the unmeasured one stays")

	pool.publishPool(map[string]uint16{"b": 20, "c": 30}, nil, true)
	require.Equal(t, []string{"b", "c"}, poolNames(pool))
	require.Equal(t, []string{"b", "c"}, poolLastNames(pool))

	pool.publishPool(map[string]uint16{"b": 20}, nil, true)
	require.Equal(t, []string{"b"}, poolNames(pool), "the final result evicts an unmeasured member")
}

// TestURLTestPoolPublishKeepsFastNodeAndClearsMeasured guards a partial publish:
// it must not reset the current fast node (the tolerance decides whether to
// switch) and must keep the suspicion of a node that has not been re-measured.
func TestURLTestPoolPublishKeepsFastNodeAndClearsMeasured(t *testing.T) {
	a := newPoolDeadProxy("a")
	pool := &URLTestPool{
		poolSize:   10,
		pool:       []string{"a"},
		fastNode:   a,
		suspicious: map[string]time.Time{"a": time.Now().Add(time.Hour), "b": time.Now().Add(time.Hour)},
	}

	pool.publishPool(map[string]uint16{"a": 20}, nil, false)

	require.NotNil(t, pool.fastNode, "a published result must not reset the fast node")
	require.Equal(t, "a", pool.fastNode.Name())
	require.NotContains(t, pool.suspicious, "a", "the measured node is no longer suspicious")
	require.Contains(t, pool.suspicious, "b", "an unmeasured node keeps its suspicion")
}

func TestURLTestPoolWaitsForFirstResult(t *testing.T) {
	release := make(chan struct{})
	log := &dialLog{}

	proxies := make([]C.Proxy, 0, 4)
	for i := 0; i < 4; i++ {
		probe := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy(fmt.Sprintf("fast-%d", i), log)}
		probe.testFn = func() (uint16, error) {
			<-release
			return 50, nil
		}
		proxies = append(proxies, probe)
	}

	pool := newColdPoolForTest(t, URLTestPoolOption{}, proxies...)

	done := make(chan error, 1)
	go func() {
		_, err := pool.DialContext(context.Background(), &C.Metadata{})
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("the dial returned before the first measured members: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the dial did not resume after the first measured members")
	}
	require.NotEmpty(t, poolNames(pool), "the dial must have waited for the first measured members")
}

func TestURLTestPoolFirstResultWaitTimesOut(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	probe := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy("slow", &dialLog{})}
	probe.testFn = func() (uint16, error) {
		<-release
		return 50, nil
	}

	pool := newColdPoolForTest(t, URLTestPoolOption{}, probe)
	pool.firstResultWait = 80 * time.Millisecond

	start := time.Now()
	conn, err := pool.DialContext(context.Background(), &C.Metadata{})
	elapsed := time.Since(start)

	require.NoError(t, err, "the dial must proceed on its own once the wait is over")
	require.NotNil(t, conn)
	require.GreaterOrEqual(t, elapsed, 80*time.Millisecond)
	require.Less(t, elapsed, time.Second)
}

func TestURLTestPoolFirstResultWaitBudgetIsPerCheck(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	probe := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy("slow", &dialLog{})}
	probe.testFn = func() (uint16, error) {
		<-release
		return 50, nil
	}

	pool := newColdPoolForTest(t, URLTestPoolOption{}, probe)
	pool.firstResultWait = 80 * time.Millisecond

	start := time.Now()
	_, err := pool.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond)

	// the check still runs and the pool is still empty, but the wait is counted
	// from the start of the check, so its budget is already spent
	start = time.Now()
	_, err = pool.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.Less(t, time.Since(start), 50*time.Millisecond, "the wait budget is counted from the start of the check")
}

func TestURLTestPoolFirstResultRespectsCallerDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	probe := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy("slow", &dialLog{})}
	probe.testFn = func() (uint16, error) {
		<-release
		return 50, nil
	}

	pool := newColdPoolForTest(t, URLTestPoolOption{}, probe)
	pool.firstResultWait = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := pool.DialContext(ctx, &C.Metadata{})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Less(t, elapsed, 1500*time.Millisecond, "the first-result wait must stay inside the caller's deadline")
}

// TestURLTestPoolWarmUpFillsThePoolBeforeADial guards the pre-warm: the pool is
// measured as soon as the config is applied, so the first connection does not
// pay for a probe of its own.
func TestURLTestPoolWarmUpFillsThePoolBeforeADial(t *testing.T) {
	probe := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy("fast", &dialLog{})}
	probe.testFn = func() (uint16, error) { return 20, nil }

	pool := newColdPoolForTest(t, URLTestPoolOption{}, probe)
	pool.WarmUp()

	require.Eventually(t, func() bool { return len(poolNames(pool)) == 1 }, 2*time.Second, 5*time.Millisecond,
		"the warm-up must fill the pool without waiting for a dial")
	require.True(t, pool.hasFullCheck.Load())
	require.Equal(t, []string{"fast"}, poolLastNames(pool))
}

// TestURLTestPoolWarmUpWaitsForProviders guards a group whose providers have not
// finished loading: the check must wait for them instead of rebuilding the pool
// from the empty fallback, and start as soon as they arrive.
func TestURLTestPoolWarmUpWaitsForProviders(t *testing.T) {
	probe := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy("fast", &dialLog{})}
	probe.testFn = func() (uint16, error) { return 20, nil }

	inner, err := provider.NewCompatibleProvider("pool", []C.Proxy{probe},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)
	pd := &delayedPoolProvider{ProxyProvider: inner, ready: make(chan struct{})}

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Interval: 300},
		URLTestPoolOption{}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"

	pool.WarmUp()

	time.Sleep(200 * time.Millisecond) // several polls of a provider that is still loading
	require.False(t, pool.hasFullCheck.Load(), "a check over an empty provider must not mark the pool checked")

	close(pd.ready)
	require.Eventually(t, func() bool { return len(poolNames(pool)) == 1 }, 2*time.Second, 5*time.Millisecond,
		"the warm-up must start the check once the provider arrives")
}

// TestURLTestPoolWarmUpWakesOnOwnProviderChange guards the subscription WarmUp
// installs: a change of one of the group's providers must end the wait at once,
// while a change of any other provider must not. The safety recheck is pushed
// out of the way, so only the subscription can end the wait in time.
func TestURLTestPoolWarmUpWakesOnOwnProviderChange(t *testing.T) {
	probe := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy("fast", &dialLog{})}
	probe.testFn = func() (uint16, error) { return 20, nil }

	inner, err := provider.NewCompatibleProvider("pool", []C.Proxy{probe},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)
	pd := &delayedPoolProvider{ProxyProvider: inner, ready: make(chan struct{})}

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Interval: 300},
		URLTestPoolOption{}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"
	pool.preWarmRecheck = 30 * time.Second

	pool.WarmUp()
	P.NotifyChanged("other")
	time.Sleep(100 * time.Millisecond)
	require.False(t, pool.hasFullCheck.Load(), "a change of another provider must not start the check")

	close(pd.ready)
	P.NotifyChanged("pool")
	require.Eventually(t, func() bool { return pool.hasFullCheck.Load() }, 3*time.Second, 5*time.Millisecond,
		"a change of the group's own provider must start the check at once")
}

// delayedPoolProvider is a provider whose proxies arrive only when the test
// releases them: an embedded core applies the config before its subscription
// providers have loaded, and the group must wait for them.
type delayedPoolProvider struct {
	P.ProxyProvider
	ready chan struct{}
}

func (d *delayedPoolProvider) Proxies() []C.Proxy {
	select {
	case <-d.ready:
		return d.ProxyProvider.Proxies()
	default:
		return nil
	}
}

// TestURLTestPoolWarmUpSkipsLazy guards the opt-out: a lazy pool runs a check
// only when real traffic asks for it.
func TestURLTestPoolWarmUpSkipsLazy(t *testing.T) {
	probe := &poolProbeProxy{cascadeFakeProxy: newPoolSucceedingProxy("fast", &dialLog{})}
	probe.testFn = func() (uint16, error) { return 20, nil }

	pd, err := provider.NewCompatibleProvider("pool", []C.Proxy{probe},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Lazy: true},
		URLTestPoolOption{}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"

	pool.WarmUp()

	require.False(t, pool.preWarming.Load(), "a lazy pool must not start a warm-up")
	require.False(t, pool.hasFullCheck.Load())
}

// The emergency test is shared with every dial that joins it, so its probes get
// the recovery timeout and not the leftover budget of the dial that started it.
func TestURLTestPoolRecoveryProbeKeepsItsTimeout(t *testing.T) {
	held := newPoolRelease()
	t.Cleanup(held.close)

	measured := newPoolControlledProxy("measured", nil, func(context.Context) (uint16, error) {
		time.Sleep(300 * time.Millisecond)
		return 10, nil
	})
	blind := newPoolControlledProxy("blind", nil, poolProbeHeld(held))

	pool := newExhaustedPoolForTest(t, measured, blind)
	pool.mu.Lock()
	pool.lastGood = "blind"
	pool.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	// the dial itself leaves on the blind fallback once the 200ms it has left
	// are gone, only the shared test keeps probing
	_, err := pool.DialContext(ctx, &C.Metadata{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		alive, _ := pool.AliveMember()
		return alive
	}, time.Second, 10*time.Millisecond,
		"a 300ms probe must survive the 200ms the starting dial had left")
	require.Equal(t, "measured", poolNames(pool)[0])
}

// A member the emergency test found still gets its dial, and when that dial
// fails the connection keeps the blind last-good attempt instead of dying.
func TestURLTestPoolMeasuredMemberFailureFallsBackToLastGood(t *testing.T) {
	log := &dialLog{}
	held := newPoolRelease()
	t.Cleanup(held.close)

	measured := newPoolControlledProxy("measured", func(context.Context, *C.Metadata) (C.Conn, error) {
		log.record("measured")
		return nil, errors.New("dial failed")
	}, poolProbeOk(10))
	blind := newPoolControlledProxy("blind", func(context.Context, *C.Metadata) (C.Conn, error) {
		log.record("blind")
		return &cascadeFakeConn{}, nil
	}, poolProbeHeld(held))

	pool := newExhaustedPoolForTest(t, measured, blind)
	pool.mu.Lock()
	pool.lastGood = "blind"
	pool.mu.Unlock()

	conn, err := pool.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Equal(t, []string{"measured", "blind"}, log.list(),
		"a measured member that failed its dial must not cost the connection the last-good attempt")
}

// A packet flow of an exhausted pool gets the same last-good fallback as a TCP
// dial when the member the emergency test found fails its flow.
func TestURLTestPoolPacketFlowFallsBackToLastGood(t *testing.T) {
	log := &dialLog{}
	held := newPoolRelease()
	t.Cleanup(held.close)

	measured := newPoolControlledProxy("measured", nil, poolProbeOk(10))
	measured.packetFn = func() error {
		log.record("measured")
		return errors.New("packet failed")
	}
	blind := newPoolControlledProxy("blind", nil, poolProbeHeld(held))
	blind.packetFn = func() error {
		log.record("blind")
		return nil
	}

	pool := newExhaustedPoolForTest(t, measured, blind)
	pool.mu.Lock()
	pool.lastGood = "blind"
	pool.mu.Unlock()

	pc, err := pool.ListenPacketContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, pc)
	require.Equal(t, []string{"measured", "blind"}, log.list(),
		"a packet flow must get the same last-good fallback as a TCP dial")
}

// A mixed pool reports UDP while any member carries it and keeps its packet
// flows on those members, so the group does not lose UDP when the fastest
// member changes to one without it.
func TestURLTestPoolPacketFlowSkipsMemberWithoutUDP(t *testing.T) {
	log := &dialLog{}

	fast := newPoolControlledProxy("fast", nil, poolProbeOk(10))
	fast.udp = false
	fast.packetFn = func() error {
		log.record("fast")
		return nil
	}
	slow := newPoolControlledProxy("slow", nil, poolProbeOk(200))
	slow.packetFn = func() error {
		log.record("slow")
		return nil
	}

	pool := newPoolForTest(t, URLTestPoolOption{}, fast, slow)
	_, _ = fast.URLTest(context.Background(), pool.testUrl, pool.expectedRanges)
	_, _ = slow.URLTest(context.Background(), pool.testUrl, pool.expectedRanges)

	require.True(t, pool.SupportUDP(), "the group carries UDP while any member does")

	pc, err := pool.ListenPacketContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, pc)
	require.Equal(t, []string{"slow"}, log.list(),
		"a packet flow must land on a member that carries UDP, not on the fastest one")
	require.Equal(t, "fast", pool.Now(), "a tcp dial still picks the fastest member")

	slow.mu.Lock()
	slow.udp = false
	slow.mu.Unlock()
	require.False(t, pool.SupportUDP(), "a pool without a UDP member must not report UDP")
}

// A member is not skipped forever after one failed dial: the mark of a
// suspicion expires on its own.
func TestURLTestPoolSuspicionExpires(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newCascadeTestProxy("a"))
	pool.suspiciousTTL = 20 * time.Millisecond
	pool.markSuspicious("a")

	pool.mu.RLock()
	require.True(t, pool.isSuspiciousLocked("a"))
	pool.mu.RUnlock()

	require.Eventually(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		return !pool.isSuspiciousLocked("a")
	}, time.Second, 5*time.Millisecond, "a suspicion must not outlive its ttl")
}

// The state a ui needs to explain a slow switch is part of the group's json.
func TestURLTestPoolMarshalJSONState(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newCascadeTestProxy("a"))

	pool.mu.Lock()
	pool.lastGood = "a"
	pool.suspicious["a"] = time.Now().Add(time.Hour)
	pool.mu.Unlock()

	data, err := pool.MarshalJSON()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, "a", decoded["lastGood"])
	require.Equal(t, false, decoded["recovering"])
	require.Equal(t, []any{"a"}, decoded["suspicious"])
}

// An explicit test of the pool's own target is the pool's own full check: it
// sweeps every candidate and rebuilds the pool, so the pool a manual test
// leaves cannot depend on which of the two ran last.
func TestURLTestPoolManualTestRunsTheOwnCheck(t *testing.T) {
	slow := newPoolControlledProxy("slow", nil, poolProbeOk(300))
	fast := newPoolControlledProxy("fast", nil, poolProbeOk(10))

	pool := newPoolForTest(t, URLTestPoolOption{}, slow, fast)
	pool.mu.Lock()
	pool.pool = []string{"slow"}
	pool.mu.Unlock()

	results, err := pool.URLTest(context.Background(), pool.testUrl, pool.expectedRanges)
	require.NoError(t, err)
	require.Equal(t, map[string]uint16{"fast": 10, "slow": 300}, results,
		"a manual test must answer with every candidate the sweep measured")
	require.Equal(t, []string{"fast", "slow"}, poolNames(pool),
		"a manual test must rebuild the pool the way a scheduled full check does")
}

// A test that lands while a check is running joins it: the pool answers with
// what that check measures instead of sweeping the same candidates again.
func TestURLTestPoolManualTestJoinsRunningCheck(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var probes atomic.Int32

	member := newPoolControlledProxy("member", nil, func(ctx context.Context) (uint16, error) {
		probes.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return 42, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	pool := newPoolForTest(t, URLTestPoolOption{}, member)
	pool.scheduleCheck(true)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the scheduled check did not start its probe")
	}

	answered := make(chan map[string]uint16, 1)
	go func() {
		results, _ := pool.URLTest(context.Background(), pool.testUrl, pool.expectedRanges)
		answered <- results
	}()

	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, probes.Load(), "a test that joins a running check must not probe again")
	require.True(t, pool.testing.Load(), "the joined test must not end the check it joined")

	close(release)
	select {
	case results := <-answered:
		require.Equal(t, map[string]uint16{"member": 42}, results)
	case <-time.After(5 * time.Second):
		t.Fatal("the joined test never answered")
	}
}

// The caller's budget bounds only its answer: a test that gives up must not cut
// the sweep nor rebuild the pool from the members it happened to measure.
func TestURLTestPoolManualTestOutlivesCaller(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)

	member := newPoolControlledProxy("member", nil, func(ctx context.Context) (uint16, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return 42, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	pool := newPoolForTest(t, URLTestPoolOption{}, member)
	pool.mu.Lock()
	pool.pool = nil
	pool.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	results, err := pool.URLTest(ctx, pool.testUrl, pool.expectedRanges)
	require.NoError(t, err)
	require.Empty(t, results, "a caller that gave up answers with what is measured, not with a verdict")

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the sweep did not reach the member")
	}
	require.True(t, pool.testing.Load(), "the caller's context must not cut the sweep")
	require.Empty(t, poolNames(pool), "a caller that gave up must not publish a pool of its own")

	close(release)
	require.Eventually(t, func() bool {
		return pool.lastResultsCopy()["member"] == 42 && len(poolNames(pool)) == 1
	}, 5*time.Second, 10*time.Millisecond, "the sweep must publish its result after the caller is gone")
}
