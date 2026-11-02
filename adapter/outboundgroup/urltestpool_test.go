package outboundgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
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

	require.False(t, pool.shouldRunFullCheck(3)) // a full pool resets the counter
	require.False(t, pool.shouldRunFullCheck(2)) // first underfilled quick check
	require.True(t, pool.shouldRunFullCheck(2))  // second one triggers a full check
	require.False(t, pool.shouldRunFullCheck(2)) // the counter has been reset
}

func TestURLTestPoolDegradedChecksDisabled(t *testing.T) {
	checks := 0
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{DegradedChecks: &checks}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Zero(t, pool.degradedChecks)

	pool.pool = []string{"a", "b"}
	require.False(t, pool.shouldRunFullCheck(1))
	require.False(t, pool.shouldRunFullCheck(1))
}

func TestURLTestPoolDegradedChecksSmallPool(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	// fewer proxies than pool-size: a fully alive small pool is not degraded
	pool.pool = []string{"a", "b"}
	require.False(t, pool.shouldRunFullCheck(2))
	require.False(t, pool.shouldRunFullCheck(2))
}

func TestURLTestPoolProxiesReady(t *testing.T) {
	emptyFallback := newPoolEmptyFallback()
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool"}, URLTestPoolOption{}, emptyFallback, nil)
	require.NoError(t, err)

	require.False(t, pool.proxiesReady([]C.Proxy{emptyFallback}))
	require.True(t, pool.proxiesReady([]C.Proxy{emptyFallback, adapter.NewProxy(outbound.NewDirect())}))
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

func TestURLTestPoolProxiesLockedKeepsPoolOrder(t *testing.T) {
	a := newPoolDeadProxy("a")
	b := newPoolDeadProxy("b")
	c := newPoolDeadProxy("c")

	pool := &URLTestPool{pool: []string{"c", "a", "removed"}}
	require.Equal(t, []string{"c", "a"}, cascadeProxyNames(pool.poolProxiesLocked([]C.Proxy{a, b, c})))
}
