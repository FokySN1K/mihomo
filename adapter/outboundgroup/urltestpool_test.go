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
	"github.com/metacubex/mihomo/adapter/outboundgroup/probequeue"
	"github.com/metacubex/mihomo/adapter/provider"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/iface"
	"github.com/metacubex/mihomo/component/reputation"
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

// A probe the caller left is not a verdict yet: PoolCheck must report the
// cancellation instead of wiping the pool the parent dials through, while the
// probe itself keeps running to its own timeout.
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
		"a probe the caller did not wait for must not be taken as a dead member")
}

// PoolCheck publishes what it measured even when the caller stops waiting, and
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

// A parent that asks an exhausted pool during the outage backoff must not buy
// a sweep of every candidate again: the canary answers it, and a canary that
// found nothing leaves the schedule exactly as it was.
func TestURLTestPoolParentCheckHonorsOutageBackoff(t *testing.T) {
	var probes atomic.Int32

	members := make([]C.Proxy, 0, 5)
	for i := 0; i < 5; i++ {
		members = append(members, newPoolControlledProxy(fmt.Sprintf("dead%02d", i), nil, func(context.Context) (uint16, error) {
			probes.Add(1)
			return 0, errors.New("probe failed")
		}))
	}

	pool := newExhaustedPoolForTest(t, members...)
	swept := time.Now().Add(-time.Hour)
	release := time.Now().Add(time.Hour)
	pool.lastFull.Store(swept)
	pool.mu.Lock()
	pool.emptyStreak = 1
	pool.nextFullAt = release
	pool.mu.Unlock()

	results, err := pool.PoolCheck(context.Background())
	require.ErrorIs(t, err, errNoAliveMember)
	require.Nil(t, results)
	require.Equal(t, int32(urlTestPoolCanaryProbes), probes.Load(),
		"a parent check during the backoff must probe the canary, not every candidate")

	lastFull, ok := pool.lastFull.LoadOk()
	require.True(t, ok)
	require.Equal(t, swept, lastFull, "a canary probe is not a full sweep and must not consume the schedule")

	pool.mu.RLock()
	defer pool.mu.RUnlock()
	require.Equal(t, 1, pool.emptyStreak, "a canary that found nothing must not deepen the backoff")
	require.Equal(t, release, pool.nextFullAt)
}

// A sweep the parent cut is not a verdict about the pool: the members the short
// budget reached must not rebuild the pool, and the cut must not count as the
// full check the schedule waits for.
func TestURLTestPoolParentCheckCutSweepIsNotAVerdict(t *testing.T) {
	held := newPoolRelease()
	t.Cleanup(held.close)

	fast := newPoolControlledProxy("fast", nil, poolProbeOk(10))
	slow := newPoolControlledProxy("slow", nil, poolProbeHeld(held))
	pool := newExhaustedPoolForTest(t, fast, slow)

	swept := time.Now().Add(-time.Hour)
	pool.lastFull.Store(swept)
	pool.lastQuick.Store(swept)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	results, err := pool.PoolCheck(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]uint16{"fast": 10}, results)
	require.Equal(t, []string{"gone"}, poolNames(pool),
		"a cut sweep must not rebuild the pool from the members one short budget reached")

	lastFull, ok := pool.lastFull.LoadOk()
	require.True(t, ok)
	require.Equal(t, swept, lastFull, "a cut sweep must not count as the scheduled full check")

	pool.mu.RLock()
	defer pool.mu.RUnlock()
	require.Zero(t, pool.emptyStreak, "a cut sweep must not start the outage backoff")
	require.True(t, pool.nextFullAt.IsZero())
}

// A canary that answers ends the outage backoff and releases the sweep the
// backoff postponed, so a parent that caught the network coming back does not
// leave the pool waiting out the rest of the backoff.
func TestURLTestPoolParentCheckCanaryAnswerReleasesTheSweep(t *testing.T) {
	members := make([]C.Proxy, 0, 4)
	for i := 0; i < 4; i++ {
		members = append(members, newPoolControlledProxy(fmt.Sprintf("alive%02d", i), nil, poolProbeOk(uint16(10+i))))
	}

	pool := newExhaustedPoolForTest(t, members...)
	swept := time.Now().Add(-2 * time.Hour)
	pool.lastFull.Store(swept)
	pool.mu.Lock()
	pool.emptyStreak = 2
	pool.nextFullAt = time.Now().Add(time.Hour)
	pool.mu.Unlock()

	results, err := pool.PoolCheck(context.Background())
	require.NoError(t, err)
	require.Len(t, results, urlTestPoolCanaryProbes)

	pool.mu.RLock()
	streak := pool.emptyStreak
	next := pool.nextFullAt
	pool.mu.RUnlock()
	require.Zero(t, streak, "a canary member that answered ends the outage backoff")
	require.True(t, next.IsZero())

	require.Eventually(t, func() bool {
		last, ok := pool.lastFull.LoadOk()
		return ok && last.After(swept)
	}, 2*time.Second, 10*time.Millisecond, "the answer must release the full sweep the backoff postponed")
	require.Len(t, poolNames(pool), 4, "the released sweep must rebuild the pool from every candidate")
}

// Every sweep of every pool shares one probe queue: the process never exceeds
// the configured number of probes, however many groups start at once.
func TestURLTestPoolSweepSharesTheQueue(t *testing.T) {
	const memberCount = 30

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	members := make([]C.Proxy, 0, memberCount)
	for i := 0; i < memberCount; i++ {
		members = append(members, newPoolControlledProxy(fmt.Sprintf("p%02d", i), nil, func(context.Context) (uint16, error) {
			running := inFlight.Add(1)
			for {
				seen := maxInFlight.Load()
				if running <= seen || maxInFlight.CompareAndSwap(seen, running) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			return 10, nil
		}))
	}

	pool := newPoolForTest(t, URLTestPoolOption{}, members...)
	_, err := pool.URLTest(context.Background(), pool.testUrl, pool.expectedRanges)
	require.NoError(t, err)
	require.LessOrEqual(t, maxInFlight.Load(), int32(probequeue.GetConcurrency()),
		"a sweep must not exceed the shared probe concurrency")
	require.Greater(t, maxInFlight.Load(), int32(1), "a sweep must still probe in parallel")
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

// poolOrderedProxy reports an aliveness and a delay of its own, so a test can
// decide which members a sweep probes first without running health checks.
type poolOrderedProxy struct {
	*cascadeFakeProxy
	alive bool
	delay uint16
}

func (p *poolOrderedProxy) AliveForTestUrl(string) bool { return p.alive }

func (p *poolOrderedProxy) LastDelayForTestUrl(string) uint16 {
	if !p.alive {
		return 0xffff
	}
	return p.delay
}

func newPoolOrderedProxy(name string, alive bool, delay uint16) *poolOrderedProxy {
	return &poolOrderedProxy{cascadeFakeProxy: newCascadeTestProxy(name), alive: alive, delay: delay}
}

// A sweep probes the members that answered last time first, so the first wave
// of an outage refills the pool instead of walking a whole provider.
func TestURLTestPoolCheckOrdersMeasuredFirst(t *testing.T) {
	pool := &URLTestPool{testUrl: poolTestUrl}

	dead := newPoolOrderedProxy("dead", false, 0)
	slow := newPoolOrderedProxy("slow", true, 400)
	fast := newPoolOrderedProxy("fast", true, 20)
	proxies := []C.Proxy{dead, slow, fast}

	require.Equal(t, []string{"fast", "slow", "dead"}, cascadeProxyNames(pool.orderForCheck(proxies)))
	require.Equal(t, []string{"dead", "slow", "fast"}, cascadeProxyNames(proxies),
		"ordering the sweep must not reorder the provider list")
}

// The emergency test replays the last pool while it answers, and moves on to
// the likeliest candidates of the whole list once that pool has missed: a
// network that changed leaves every member of the old pool dead.
func TestURLTestPoolRecoveryScansAfterMiss(t *testing.T) {
	proxies := make([]C.Proxy, 0, urlTestPoolRecoveryScanLimit+5)
	for i := 0; i < urlTestPoolRecoveryScanLimit+5; i++ {
		proxies = append(proxies, newPoolOrderedProxy(fmt.Sprintf("p%02d", i), true, uint16(i)))
	}

	pool := &URLTestPool{testUrl: poolTestUrl, lastPool: []string{"p23", "p24"}}
	require.Equal(t, []string{"p23", "p24"}, cascadeProxyNames(pool.recoveryCandidates(proxies)),
		"a pool that has not missed yet replays its own members")

	pool.recoveryMisses = 1
	scan := pool.recoveryCandidates(proxies)
	require.Len(t, scan, urlTestPoolRecoveryScanLimit, "the scan must stay bounded")
	require.Equal(t, "p00", scan[0].Name())
	require.Equal(t, fmt.Sprintf("p%02d", urlTestPoolRecoveryScanLimit-1), scan[len(scan)-1].Name())
}

// A dial with nothing measured alive waits for the running sweep to publish its
// first alive member instead of spending the budget on a dial the pool already
// knows is dead.
func TestURLTestPoolExhaustedDialWaitsForTheSweep(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newPoolOrderedProxy("dead", false, 0), newPoolOrderedProxy("found", true, 10))
	pool.mu.Lock()
	pool.pool = []string{"dead"}
	pool.mu.Unlock()
	pool.firstResultWait = 5 * time.Second
	pool.beginWarmup()

	measured := make(chan C.Proxy, 1)
	go func() {
		measured <- pool.awaitCheck(context.Background())
	}()

	select {
	case <-measured:
		t.Fatal("a dial must wait for the sweep while nothing is measured alive")
	case <-time.After(20 * time.Millisecond):
	}

	// the sweep publishes its first alive member
	pool.mu.Lock()
	pool.pool = []string{"found"}
	pool.mu.Unlock()
	pool.markWarm()

	select {
	case proxy := <-measured:
		require.NotNil(t, proxy)
		require.Equal(t, "found", proxy.Name())
	case <-time.After(time.Second):
		t.Fatal("the dial must leave on the first alive member of the sweep")
	}
}

// Without a running sweep there is no result to wait for.
func TestURLTestPoolExhaustedDialDoesNotWaitWithoutSweep(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newPoolOrderedProxy("dead", false, 0))
	pool.firstResultWait = 5 * time.Second

	start := time.Now()
	require.Nil(t, pool.awaitCheck(context.Background()))
	require.Less(t, time.Since(start), 100*time.Millisecond)
}

// A member cut by our own attempt-timeout is not proof of death: it leaves the
// next choices only for the grace instead of the whole interval a failed check
// earns.
func TestURLTestPoolAttemptTimeoutCutKeepsTheMemberForTheGrace(t *testing.T) {
	hanging := newCascadeFakeProxy("hanging", C.Shadowsocks, func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil, 0)
	log := &dialLog{}
	pool := newPoolForTest(t, URLTestPoolOption{AttemptTimeout: intPtr(60)}, hanging, newPoolSucceedingProxy("b", log))

	_, err := pool.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, log.list())

	pool.mu.RLock()
	until, marked := pool.cutMarks["hanging"]
	_, reported := pool.suspicious["hanging"]
	pool.mu.RUnlock()
	require.True(t, marked, "a cut member leaves the next choices")
	require.False(t, reported, "a cut is not a verdict a host reports as suspicious")
	require.LessOrEqual(t, time.Until(until), urlTestPoolCutGrace+time.Second,
		"a cut member must come back after the grace, not sit out the whole interval")
}

// A cut says nothing about the node, so it must not shorten the interval a
// failed dial earned: the member keeps sitting out the whole suspicion.
func TestURLTestPoolCutDoesNotShortenAFailureMark(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newCascadeTestProxy("a"))

	pool.markSuspicious("a")
	pool.mu.RLock()
	failedUntil := pool.suspicious["a"]
	pool.mu.RUnlock()

	pool.markCut("a")

	pool.mu.RLock()
	defer pool.mu.RUnlock()
	require.Equal(t, failedUntil, pool.suspicious["a"], "a cut must not touch the failure mark")
	require.True(t, pool.isSuspiciousLocked("a"))
	require.WithinDuration(t, failedUntil, time.Now().Add(pool.suspiciousTTL), time.Second,
		"the failure mark keeps its whole interval")
}

// A member that answers a check is back at once, whether the mark that kept it
// out was a failure or a cut.
func TestURLTestPoolMeasuredMemberDropsItsCutMark(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newPoolControlledProxy("a", nil, poolProbeOk(10)))
	pool.markCut("a")

	pool.mu.Lock()
	measured := pool.isSuspiciousLocked("a")
	pool.mu.Unlock()
	require.True(t, measured, "the cut keeps the member out first")

	pool.quickCheck()

	pool.mu.RLock()
	defer pool.mu.RUnlock()
	require.False(t, pool.isSuspiciousLocked("a"), "a member a check measured alive is back at once")
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

// UsableMembers is what a parent reads: only the members that can carry traffic
// right now count, so a pool whose last member failed answers that it has
// nothing left.
func TestURLTestPoolUsableMembers(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newCascadeTestProxy("a"), newCascadeTestProxy("b"))
	require.Equal(t, 2, pool.UsableMembers())

	pool.markCut("a")
	require.Equal(t, 1, pool.UsableMembers(), "a cut member leaves the count for the grace")

	pool.markSuspicious("b")
	require.Equal(t, 0, pool.UsableMembers(), "a member that failed a dial does not count")
}

// A dial the caller's budget cut is not a verdict on the member: it only earns
// the grace, while a caller that canceled on its own earns nothing at all.
func TestURLTestPoolCallerDeadlineMarksOnlyTheGrace(t *testing.T) {
	hang := func(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	pool := newPoolForTest(t, URLTestPoolOption{}, newCascadeFakeProxy("hanging", C.Shadowsocks, hang, nil, 0))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := pool.DialContext(ctx, &C.Metadata{})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	pool.mu.RLock()
	until, cut := pool.cutMarks["hanging"]
	_, reported := pool.suspicious["hanging"]
	pool.mu.RUnlock()
	require.True(t, cut, "a dial the caller cut leaves the member only for the grace")
	require.False(t, reported, "a cut must not earn the whole interval a failed dial does")
	require.LessOrEqual(t, time.Until(until), urlTestPoolCutGrace+time.Second)

	canceled := newPoolForTest(t, URLTestPoolOption{}, newCascadeFakeProxy("other", C.Shadowsocks, hang, nil, 0))
	canceledCtx, cancelDial := context.WithCancel(context.Background())
	cancelDial()

	_, err = canceled.DialContext(canceledCtx, &C.Metadata{})
	require.Error(t, err)

	canceled.mu.RLock()
	defer canceled.mu.RUnlock()
	require.Empty(t, canceled.suspicious, "a caller that canceled on its own says nothing about the member")
	require.Empty(t, canceled.cutMarks)
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
	// every test gets its own history, so the order of the tests cannot leak
	// into the reputation of a member of another pool
	pool.reputation = reputation.NewStore()
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
	pool := newPoolForTest(t, URLTestPoolOption{AttemptTimeout: intPtr(0)},
		newPoolFailingProxy("a", log),
		newPoolSucceedingProxy("b", log),
	)

	_, err := pool.DialContext(context.Background(), &C.Metadata{})
	require.Error(t, err)
	require.Equal(t, []string{"a"}, log.list(), "attempt-timeout 0 keeps one dial per call")
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
		URLTestPoolOption{AttemptTimeout: intPtr(0)}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Zero(t, pool.attemptTimeout, "an explicit 0 opts out of the bound")

	pool, err = NewURLTestPool(GroupCommonOption{Name: "pool"},
		URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Equal(t, defaultURLTestPoolAttemptTimeout, pool.attemptTimeout,
		"without the option one hanging member costs the whole dial budget")
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
	// The pool is published as the first result arrives, the marker of a
	// finished check a few steps later, so it is waited for as well.
	require.Eventually(t, pool.hasFullCheck.Load, 2*time.Second, 5*time.Millisecond)
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

// TestURLTestPoolWarmUpMeasuresLazyLeaders guards the lazy warm-up: the profile
// asked for checks driven by traffic, so the group does not sweep its
// candidates at a start, but it still measures the members the reputation
// trusts, and only those. The first dial then routes by a real measurement
// without paying for a check of its own.
func TestURLTestPoolWarmUpMeasuresLazyLeaders(t *testing.T) {
	var probes atomic.Int32
	members := make([]C.Proxy, 0, 4)
	for i := 0; i < 4; i++ {
		members = append(members, newPoolControlledProxy(
			fmt.Sprintf("member-%d", i), nil, func(context.Context) (uint16, error) {
				probes.Add(1)
				return 20, nil
			}))
	}

	pd, err := provider.NewCompatibleProvider("pool", members,
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Lazy: true, Interval: 300},
		URLTestPoolOption{PoolSize: 2}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"

	// the reputation trusts the two last members, so they are the ones worth
	// the probes; the rest waits for the sweep the traffic starts
	for i := 0; i < 5; i++ {
		pool.reputation.DialSucceeded("member-3")
	}
	pool.reputation.DialSucceeded("member-2")

	pool.WarmUp()

	require.Eventually(t, func() bool { return len(poolNames(pool)) == 2 }, 2*time.Second, 5*time.Millisecond,
		"the warm-up must fill a lazy pool before the first dial")
	require.ElementsMatch(t, []string{"member-2", "member-3"}, poolNames(pool))
	require.Equal(t, int32(2), probes.Load(), "a lazy warm-up measures the pool size, not every candidate")
	require.Eventually(t, pool.hasFullCheck.Load, 2*time.Second, 5*time.Millisecond,
		"the warm-up must count as the first measurement of the pool")
}

// A lazy pool whose reputation leaders do not answer keeps the slow path: the
// first dial starts its own check instead of inheriting an empty sweep.
func TestURLTestPoolWarmUpLeavesDeadLazyPoolToTheDial(t *testing.T) {
	var probes atomic.Int32
	dead := newPoolControlledProxy("dead", nil, func(context.Context) (uint16, error) {
		probes.Add(1)
		return 0, errors.New("probe failed")
	})

	pd, err := provider.NewCompatibleProvider("pool", []C.Proxy{dead},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Lazy: true, Interval: 300},
		URLTestPoolOption{}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"

	pool.WarmUp()

	require.Eventually(t, func() bool { return probes.Load() == 1 && !pool.testing.Load() }, time.Second, 5*time.Millisecond,
		"the warm-up must probe the reputation leaders")
	require.False(t, pool.hasFullCheck.Load(), "a warm-up that measured nothing must not mark the pool checked")
	require.Empty(t, poolNames(pool))
	pool.mu.RLock()
	require.Zero(t, pool.nextFullAt, "a warm-up that measured nothing must not start an outage backoff")
	pool.mu.RUnlock()
}

// TestURLTestPoolLazyReadStartsNoCheck guards the read path: a lazy pool stays
// silent until real traffic asks for a check, even on the very first read of
// its state after a start. A host polls the group list right away, and that
// read must not wake a pool nothing dials through yet.
func TestURLTestPoolLazyReadStartsNoCheck(t *testing.T) {
	var probes atomic.Int32
	member := newPoolControlledProxy("member", nil, func(context.Context) (uint16, error) {
		probes.Add(1)
		return 10, nil
	})

	pd, err := provider.NewCompatibleProvider("pool", []C.Proxy{member},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Lazy: true, Interval: 300},
		URLTestPoolOption{}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"

	// the members are listed but nothing has been measured yet
	pool.pool = []string{"member"}
	require.Equal(t, "member", pool.Now())
	_, err = pool.MarshalJSON()
	require.NoError(t, err)

	// the empty pool a lazy start begins with: the read must not reach for the
	// emergency full check either
	pool.pool = nil
	require.Equal(t, "member", pool.Now())
	_, err = pool.MarshalJSON()
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	require.False(t, pool.testing.Load(), "reading a lazy pool must not start a check")
	require.False(t, pool.hasFullCheck.Load())
	require.Zero(t, probes.Load())
}

// TestURLTestPoolLazyDialStartsCheck keeps the other half of the contract: a
// lazy pool still checks as soon as real traffic asks for it.
func TestURLTestPoolLazyDialStartsCheck(t *testing.T) {
	var probes atomic.Int32
	held := newPoolRelease()
	t.Cleanup(held.close)

	member := newPoolControlledProxy("member", nil, func(ctx context.Context) (uint16, error) {
		probes.Add(1)
		return poolProbeHeld(held)(ctx)
	})

	pd, err := provider.NewCompatibleProvider("pool", []C.Proxy{member},
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)

	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Lazy: true, Interval: 300},
		URLTestPoolOption{}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	pool.testUrl = "http://127.0.0.1:1/"

	pool.fastForDial(true)

	require.Eventually(t, func() bool { return probes.Load() > 0 }, time.Second, 5*time.Millisecond,
		"a lazy pool must check when real traffic asks for it")
	require.True(t, pool.testing.Load())
}

// The emergency scan must probe the nodes that carried traffic before the ones
// that have been failing, so an outage is resolved by the first wave instead of
// after a sweep of the whole provider.
func TestURLTestPoolOrderForCheckPrefersTrusted(t *testing.T) {
	trusted := newPoolControlledProxy("rep-trusted", nil, poolProbeOk(10))
	dead := newPoolControlledProxy("rep-dead", nil, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{}, trusted, dead)

	for i := 0; i < 3; i++ {
		pool.reputation.DialSucceeded("rep-trusted")
	}
	pool.reputation.DialFailed("rep-dead")

	ordered := pool.orderForCheck(pool.GetProxies(false))
	require.Equal(t, "rep-trusted", ordered[0].Name())
	require.Equal(t, "rep-dead", ordered[len(ordered)-1].Name())
}

// A pool with nothing measured must dial the node with the best reputation
// instead of the first one of the list.
func TestURLTestPoolColdPickUsesReputation(t *testing.T) {
	first := newPoolControlledProxy("rep-first", nil, poolProbeOk(10))
	trusted := newPoolControlledProxy("rep-best", nil, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{}, first, trusted)
	pool.hasFullCheck.Store(false) // the members have never been measured

	for i := 0; i < 4; i++ {
		pool.reputation.DialSucceeded("rep-best")
	}

	proxy, _ := pool.fastForDial(false)
	require.Equal(t, "rep-best", proxy.Name())
}

// A failed probe is evidence too: a node that does not answer a health check
// sinks in the store without waiting for a real dial to fail.
func TestURLTestPoolProbeFailureLowersReputation(t *testing.T) {
	broken := newPoolControlledProxy("rep-broken", nil, func(context.Context) (uint16, error) {
		return 0, errors.New("probe failed")
	})
	alive := newPoolControlledProxy("rep-alive", nil, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{}, broken, alive)

	_, err := pool.PoolCheck(context.Background())
	require.NoError(t, err)
	require.Equal(t, -10, pool.reputation.Score("rep-broken"))
	require.Zero(t, pool.reputation.Score("rep-alive"), "a measured member must not be charged")
}

// A sweep that measured nothing alive is a verdict on the route, not on the
// nodes: an outage takes the whole candidate list down at once, and the store
// must not sink every member for it.
func TestURLTestPoolEmptySweepKeepsReputation(t *testing.T) {
	var probes atomic.Int32
	dead := func(name string) C.Proxy {
		return newPoolControlledProxy(name, nil, func(context.Context) (uint16, error) {
			probes.Add(1)
			return 0, errors.New("probe failed")
		})
	}
	pool := newPoolForTest(t, URLTestPoolOption{}, dead("rep-a"), dead("rep-b"))

	_, err := pool.PoolCheck(context.Background())
	require.Error(t, err, "a check that found nothing alive reports it")
	require.Equal(t, int32(2), probes.Load(), "both candidates must be measured")
	require.Zero(t, pool.reputation.Score("rep-a"))
	require.Zero(t, pool.reputation.Score("rep-b"))
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

// A member that carried a connection is not suspicious, whatever mark an
// earlier dial or a sweep of a dead network left on it.
func TestURLTestPoolDialClearsMarks(t *testing.T) {
	carried := newPoolControlledProxy("carried", nil, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{}, carried)

	pool.markSuspicious("carried")
	pool.markCut("carried")

	pool.noteDialAccepted(pool.GetProxies(false)[0])

	pool.mu.RLock()
	defer pool.mu.RUnlock()
	require.False(t, pool.isSuspiciousLocked("carried"), "a dial that carried traffic clears the suspicion")
	require.Empty(t, pool.cutMarks, "a dial that carried traffic clears the cut grace")
	require.Equal(t, "carried", pool.lastGood)
}

// The state a ui needs to explain a slow switch is part of the group's json.
func TestURLTestPoolMarshalJSONState(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newCascadeTestProxy("a"))

	pool.mu.Lock()
	pool.lastGood = "a"
	pool.suspicious["a"] = time.Now().Add(time.Hour)
	pool.cutMarks["b"] = time.Now().Add(time.Hour)
	pool.mu.Unlock()

	data, err := pool.MarshalJSON()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, "a", decoded["lastGood"])
	require.Equal(t, false, decoded["recovering"])
	require.Equal(t, []any{"a"}, decoded["suspicious"],
		"a member only cut by our own timeout is not reported as suspicious")
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
	pool.scheduleCheck(poolFullCheck)
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

// poolOutageFixture builds a pool of members that count their probes, so a
// test can tell a full sweep of every candidate from a canary probe of a few.
func poolOutageFixture(t *testing.T, members int, alive ...string) (*URLTestPool, *atomic.Int32) {
	t.Helper()

	answering := make(map[string]struct{}, len(alive))
	for _, name := range alive {
		answering[name] = struct{}{}
	}

	var probes atomic.Int32
	proxies := make([]C.Proxy, 0, members)
	for i := 0; i < members; i++ {
		name := fmt.Sprintf("n%d", i)
		_, ok := answering[name]
		proxies = append(proxies, newPoolControlledProxy(name, nil, func(context.Context) (uint16, error) {
			probes.Add(1)
			if ok {
				return 20, nil
			}
			return 0, errors.New("dead")
		}))
	}

	pool := newPoolForTest(t, URLTestPoolOption{FullInterval: 600}, proxies...)
	pool.backoffJitter = func(time.Duration) time.Duration { return 0 }
	return pool, &probes
}

// emptyPoolForOutage puts the pool into the state an empty full sweep leaves:
// no member, the backoff set, and both schedules due.
func emptyPoolForOutage(pool *URLTestPool, streak int) {
	// a sweep has read the proxy list before it runs, so the provider versions
	// are cached and the backoff is not dropped as "a new list arrived"
	pool.GetProxies(false)
	pool.mu.Lock()
	pool.pool = nil
	pool.emptyStreak = streak
	pool.nextFullAt = time.Now().Add(10 * time.Minute)
	pool.mu.Unlock()
	pool.lastFull.Store(time.Now().Add(-pool.fullInterval - time.Second))
	pool.lastQuick.Store(time.Now().Add(-time.Hour))
}

// An empty full sweep must postpone the next one, every empty sweep must
// double the wait up to the cap, and a sweep that finds a member must end the
// backoff.
func TestURLTestPoolEmptySweepEntersBackoff(t *testing.T) {
	pool, _ := poolOutageFixture(t, 2)

	pool.fullCheck()
	require.Equal(t, 1, pool.emptySweepStreak())
	require.InDelta(t, pool.fullInterval.Seconds(), time.Until(pool.nextFullTime()).Seconds(), 2,
		"the first empty sweep must postpone the next full check by the full interval")

	pool.fullCheck()
	require.Equal(t, 2, pool.emptySweepStreak())
	require.InDelta(t, (2 * pool.fullInterval).Seconds(), time.Until(pool.nextFullTime()).Seconds(), 2,
		"every empty sweep must double the wait")

	pool.mu.Lock()
	for streak := 1; streak <= 8; streak++ {
		pool.emptyStreak = streak
		wait := pool.fullBackoffLocked()
		expected := pool.fullInterval << (streak - 1)
		if expected > urlTestPoolMaxFullBackoff {
			expected = urlTestPoolMaxFullBackoff
		}
		require.Equal(t, expected, wait)
	}
	pool.mu.Unlock()
}

func (u *URLTestPool) nextFullTime() time.Time {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.nextFullAt
}

func TestURLTestPoolAliveSweepLeavesBackoff(t *testing.T) {
	var alive atomic.Bool
	member := newPoolControlledProxy("member", nil, func(context.Context) (uint16, error) {
		if alive.Load() {
			return 20, nil
		}
		return 0, errors.New("dead")
	})
	pool := newPoolForTest(t, URLTestPoolOption{FullInterval: 600}, member)
	pool.backoffJitter = func(time.Duration) time.Duration { return 0 }

	pool.fullCheck()
	require.Equal(t, 1, pool.emptySweepStreak())

	alive.Store(true)
	pool.fullCheck()
	require.Zero(t, pool.emptySweepStreak(), "a member that answers must end the backoff")
	require.True(t, pool.nextFullTime().IsZero())
	require.Equal(t, []string{"member"}, poolNames(pool))
}

// While the backoff holds, a due full sweep must not probe every candidate:
// the canary measures a few of them and leaves the schedule alone.
func TestURLTestPoolBackoffPostponesFullSweepToCanary(t *testing.T) {
	pool, probes := poolOutageFixture(t, 6)
	emptyPoolForOutage(pool, 1)

	pool.maybeScheduleCheck()

	require.Eventually(t, func() bool {
		return !pool.testing.Load() && probes.Load() > 0
	}, 2*time.Second, 5*time.Millisecond, "the postponed sweep must still run its canary")

	require.EqualValues(t, urlTestPoolCanaryProbes, probes.Load(),
		"a postponed full sweep must probe the canary only, not every candidate")
	require.Equal(t, 1, pool.emptySweepStreak(), "a canary that finds nothing must not push the next sweep out")
	require.False(t, pool.nextFullTime().IsZero(), "the postponed sweep must keep its time")
}

// A canary that answers means the network is back: the full sweep the backoff
// postponed must run and rebuild the pool.
func TestURLTestPoolCanaryReleasesFullSweep(t *testing.T) {
	pool, probes := poolOutageFixture(t, 6, "n0", "n1", "n2")
	emptyPoolForOutage(pool, 1)

	pool.maybeScheduleCheck()

	require.Eventually(t, func() bool {
		return pool.emptySweepStreak() == 0 && len(poolNames(pool)) == 3
	}, 5*time.Second, 10*time.Millisecond,
		"a canary answer must release the full sweep and rebuild the pool")
	require.GreaterOrEqual(t, probes.Load(), int32(6), "the released sweep must measure every candidate")
	require.True(t, pool.nextFullTime().IsZero())
}

func TestURLTestPoolQuickIntervalStretchesWhileNothingAlive(t *testing.T) {
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Interval: 300}, URLTestPoolOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)

	require.Equal(t, 300*time.Second, pool.quickInterval())
	pool.emptyStreak = 1
	require.Equal(t, 600*time.Second, pool.quickInterval())
	pool.emptyStreak = 2
	require.Equal(t, 1200*time.Second, pool.quickInterval())
	pool.emptyStreak = 3
	require.Equal(t, urlTestPoolMaxQuickBackoff, pool.quickInterval(), "the stretch is capped")
	pool.emptyStreak = 10
	require.Equal(t, urlTestPoolMaxQuickBackoff, pool.quickInterval())
}

func TestURLTestPoolNetworkChangeDropsBackoff(t *testing.T) {
	pool, _ := poolOutageFixture(t, 2)
	pool.fullCheck()
	require.Equal(t, 1, pool.emptySweepStreak())

	pool.mu.Lock()
	pool.suspicious["n0"] = time.Now().Add(time.Hour)
	pool.cutMarks["n1"] = time.Now().Add(time.Hour)
	pool.mu.Unlock()

	// without a new generation the pool keeps both the backoff and the marks
	pool.dropBackoffOnNetworkChange()
	require.Equal(t, 1, pool.emptySweepStreak())
	require.NotEmpty(t, pool.suspicious)
	require.NotEmpty(t, pool.cutMarks)

	iface.NotifyNetworkChanged()
	pool.dropBackoffOnNetworkChange()
	require.Zero(t, pool.emptySweepStreak())
	require.True(t, pool.nextFullTime().IsZero(), "a network change must let the next full sweep run at once")
	require.Empty(t, pool.suspicious, "the marks of the old network must go")
	require.Empty(t, pool.cutMarks, "a cut on the old network must not hold a member back")

	// a second call without a new generation must keep the state as it is
	pool.mu.Lock()
	pool.emptyStreak = 2
	pool.mu.Unlock()
	pool.dropBackoffOnNetworkChange()
	require.Equal(t, 2, pool.emptySweepStreak())
}

// The first traffic after the device moved to another network measures the
// pool again at once: every member was measured on the old route, so waiting
// for the schedule of the old one would leave the pool unproven on the new
// network.
func TestURLTestPoolNetworkChangeStartsFullCheck(t *testing.T) {
	var probes atomic.Int32
	member := newPoolControlledProxy("member", nil, func(context.Context) (uint16, error) {
		probes.Add(1)
		return 20, nil
	})
	pool := newPoolForTest(t, URLTestPoolOption{FullInterval: 600}, member)

	pool.maybeScheduleCheck()
	require.False(t, pool.testing.Load(), "the schedule of a pool measured a moment ago holds")
	require.Zero(t, probes.Load())

	iface.NotifyNetworkChanged()
	pool.maybeScheduleCheck()

	require.Eventually(t, func() bool {
		return !pool.testing.Load() && probes.Load() == 1
	}, 5*time.Second, 10*time.Millisecond, "a network change must measure the pool at once")
	require.Equal(t, []string{"member"}, poolNames(pool))
}

// A check that is already running when the device moves measures the old
// route: the sweep asked for by the change must follow it instead of being lost
// with the generation it belonged to.
func TestURLTestPoolNetworkChangeSweepsAfterTheRunningCheck(t *testing.T) {
	held := newPoolRelease()
	defer held.close()
	var probes atomic.Int32
	member := newPoolControlledProxy("member", nil, func(ctx context.Context) (uint16, error) {
		if probes.Add(1) == 1 {
			// the first check runs on the old route until the test lets it go
			select {
			case <-held.ch:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		return 20, nil
	})
	pool := newPoolForTest(t, URLTestPoolOption{FullInterval: 600}, member)

	pool.scheduleCheck(poolFullCheck)
	require.Eventually(t, func() bool { return pool.testing.Load() }, 2*time.Second, 5*time.Millisecond)

	iface.NotifyNetworkChanged()
	pool.maybeScheduleCheck()
	require.True(t, pool.pendingFull.Load(), "the sweep of the new route must wait for the running check")

	held.close()

	require.Eventually(t, func() bool {
		return !pool.testing.Load() && probes.Load() >= 2
	}, 5*time.Second, 10*time.Millisecond,
		"the check of the old route must be followed by a sweep of the new one")
	require.Equal(t, []string{"member"}, poolNames(pool))
	require.False(t, pool.pendingFull.Load())
}

// The first scheduled check on a new network gives the members that failed one
// second attempt: a handshake that timed out while the route was settling is
// not a verdict, so the member must not leave the pool on the first answer.
func TestURLTestPoolFirstCheckAfterNetworkChangeRetriesFailed(t *testing.T) {
	var probes atomic.Int32
	flake := newPoolControlledProxy("flake", nil, func(context.Context) (uint16, error) {
		if probes.Add(1) == 1 {
			return 0, errors.New("the route is not settled yet")
		}
		return 20, nil
	})
	pool := newPoolForTest(t, URLTestPoolOption{}, flake)

	iface.NotifyNetworkChanged()
	pool.fullCheck()

	require.Equal(t, []string{"flake"}, poolNames(pool),
		"the member that answered on the retry must stay in the pool")
	require.EqualValues(t, 2, probes.Load(),
		"the first check on a new network must retry the failed member once")

	pool.fullCheck()
	require.EqualValues(t, 3, probes.Load(), "a check on the same generation must not retry")
}

// A member that fails both attempts earns one failure, and the retry is not
// repeated: only the first check after the network change pays for it.
func TestURLTestPoolScheduledCheckRetriesFailedOnce(t *testing.T) {
	var probes atomic.Int32
	dead := newPoolControlledProxy("dead", nil, func(context.Context) (uint16, error) {
		probes.Add(1)
		return 0, errors.New("dead")
	})
	pool := newPoolForTest(t, URLTestPoolOption{}, dead)

	iface.NotifyNetworkChanged()
	pool.fullCheck()
	require.EqualValues(t, 2, probes.Load(), "the failed member must get one second attempt")
	require.Empty(t, poolNames(pool))

	pool.fullCheck()
	require.EqualValues(t, 3, probes.Load(), "a later check on the same network must not retry")
}

func TestURLTestPoolProviderUpdateDropsBackoff(t *testing.T) {
	pd := &mutableProvider{name: "pd"}
	pd.set(newPoolDeadProxy("a"))
	pool, err := NewURLTestPool(GroupCommonOption{Name: "pool", Interval: 300},
		URLTestPoolOption{FullInterval: 600}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(t, err)
	require.Len(t, pool.GetProxies(false), 1) // cache the provider version

	pool.mu.Lock()
	pool.emptyStreak = 2
	pool.nextFullAt = time.Now().Add(time.Hour)
	pool.mu.Unlock()
	pool.dropBackoffOnProviderUpdate()
	require.Equal(t, 2, pool.emptySweepStreak(), "an unchanged provider must keep the backoff")

	pd.set(newPoolDeadProxy("b"))
	pool.dropBackoffOnProviderUpdate()
	require.Zero(t, pool.emptySweepStreak(), "a new proxy list must drop the backoff the old nodes earned")
	require.True(t, pool.nextFullTime().IsZero())
}

// The suspect band of the reputation becomes a skip of the sweep after two
// empty sweeps, but every other sweep must measure those nodes again.
func TestURLTestPoolPrunesSuspectsAfterEmptySweeps(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{},
		newPoolDeadProxy("good"), newPoolDeadProxy("suspect"), newPoolDeadProxy("other"))
	pool.reputation.DialFailed("suspect")
	pool.reputation.DialFailed("suspect")
	require.Less(t, pool.reputation.Score("suspect"), reputation.Suspect)

	pool.emptyStreak = 1
	require.Len(t, pool.orderForCheck(pool.Proxies()), 3, "one empty sweep is not enough to skip a node")

	pool.emptyStreak = 2
	require.Equal(t, []string{"good", "other"}, cascadeProxyNames(pool.orderForCheck(pool.Proxies())))

	pool.emptyStreak = 3
	require.Len(t, pool.orderForCheck(pool.Proxies()), 3, "every other sweep measures the suspect band again")
}
