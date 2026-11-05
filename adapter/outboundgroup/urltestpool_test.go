package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
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
		suspicious: map[string]struct{}{"a": {}, "b": {}},
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
