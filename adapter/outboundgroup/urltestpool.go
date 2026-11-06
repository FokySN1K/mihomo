package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"

	"golang.org/x/sync/errgroup"
)

const (
	defaultURLTestPoolSize           = 10
	defaultURLTestPoolInterval       = 300 * time.Second
	defaultURLTestPoolFullInterval   = 1800 * time.Second
	defaultURLTestPoolDegradedChecks = 3
	urlTestPoolCheckLimit            = 10
	urlTestPoolRecoveryCooldown      = 30 * time.Second
	urlTestPoolRecoveryTestInterval  = 5 * time.Second
	urlTestPoolRecoveryTestTimeout   = 2 * time.Second
	urlTestPoolRetryReserve          = 500 * time.Millisecond
	// urlTestPoolRecoveryWait is the head start an exhausted dial gives the
	// emergency test before it falls back to the last known good proxy. The
	// first probe of the test usually answers within it, so the dial leaves on
	// a measured member instead of paying for a blind dial of the old one.
	urlTestPoolRecoveryWait = 500 * time.Millisecond
	// defaultURLTestPoolFirstResultWait caps how long a dial of a cold start
	// waits for the first measured member. The wait is also bounded by the probe
	// timeout (a result cannot arrive sooner) and is counted from the start of
	// the check, so a check cycle never delays dials longer than this.
	defaultURLTestPoolFirstResultWait = 2 * time.Second
	// urlTestPoolPreWarmRecheck is the safety net of a pre-warm: a load of a
	// provider of the group wakes the wait at once, and this only covers a load
	// that finished before the group subscribed.
	urlTestPoolPreWarmRecheck = time.Second
	// urlTestPoolPreWarmWait bounds that wait. A provider that takes longer
	// leaves the group on the old path, where the first dial starts the check.
	urlTestPoolPreWarmWait = 30 * time.Second
)

type URLTestPoolOption struct {
	PoolSize     int `group:"pool-size,omitempty"`
	FullInterval int `group:"full-interval,omitempty"`
	// DegradedChecks is how many consecutive quick checks must see the pool
	// underfilled (fewer alive members than it currently holds) before an early
	// full check rebuilds it. It is not a target number of alive proxies, and
	// 0 disables the early refill.
	DegradedChecks *int `group:"degraded-checks,omitempty"`
	// Tolerance is how much faster a measured member must be than the current
	// fast node to replace it, in milliseconds; 0 disables the hysteresis.
	Tolerance *uint16 `group:"tolerance,omitempty"`
	// DialTimeout and DialRetryLimit are no longer used: the tunnel applies its
	// own budget to every group. They are kept only to warn about profiles that
	// still set them.
	DialTimeout    *int `group:"dial-timeout,omitempty"`
	DialRetryLimit *int `group:"dial-retry-limit,omitempty"`
	// AttemptTimeout bounds one pool member dial, in milliseconds. When it is
	// set, a failed member is followed by the next one while the caller's
	// budget lasts; 0 keeps the single dial per call.
	AttemptTimeout *int `group:"attempt-timeout,omitempty"`
}

// URLTestPool keeps a fixed pool of the fastest proxies.
// A quick health check tests only the pool members every `interval`,
// a full health check tests all proxies every `full-interval` and rebuilds
// the pool. When the pool stays underfilled (some members are dead) for
// several quick checks in a row, a full check is started early to refill it.
// The pool also fills itself before the first dial: the executor asks it to
// warm up as soon as the config is applied, so the first connection does not
// pay for the cold-start probe. On a cold start every measured member is
// published as soon as its probe returns, and a dial waits only for the first
// result (bounded by `timeout` and a 2s cap), so it is not routed to the first
// proxy of the list only because nothing has been measured.
// Dial failures mark the used proxy as suspicious so the next connection is
// routed to the next best pool member; with `attempt-timeout` set, the next
// member is tried inside the same call as well.
// When no member is measured alive at all (an outage, a network that changed),
// the dial starts an emergency test of the last pool before it falls back to
// the last known good proxy, and leaves on the first member that test finds.
// A packet flow chooses among the members that carry UDP, and the group reports
// UDP support as long as any member has it.
type URLTestPool struct {
	*GroupBase
	disableUDP      bool
	testUrl         string
	expectedStatus  string
	expectedRanges  utils.IntRanges[uint16]
	interval        time.Duration
	fullInterval    time.Duration
	poolSize        int
	degradedChecks  int
	tolerance       uint16
	firstResultWait time.Duration
	attemptTimeout  time.Duration
	recoveryWait    time.Duration
	lazy            bool

	mu       sync.RWMutex
	selected string
	pool     []string
	lastPool []string
	lastGood string
	// suspicious holds the members that failed a dial, with the time the mark
	// expires: a member is not skipped forever, and a check that does not reach
	// it must not keep it out of the pool either.
	suspicious    map[string]time.Time
	suspiciousTTL time.Duration
	fastNode      C.Proxy
	// measuring is the emergency test of lastPool that runs while the pool has
	// nothing measured alive. Every dial of the exhausted pool joins it instead
	// of probing on its own; it is cleared as the test ends.
	measuring     *poolMeasurement
	lastEmptyFull time.Time
	degradedCount int
	// warmCh is open while a full check is running and is closed once the first
	// measured member is published; checkStart anchors the first-result wait, so
	// the whole check cycle never delays dials longer than the cap.
	warmCh     chan struct{}
	checkStart time.Time

	// preWarming guards the single pre-warm of the group: the executor asks
	// for it on every config apply, and only the first ask may start a wait.
	preWarming atomic.Bool
	// preWarmRecheck is the safety recheck of the pre-warm wait. It only covers
	// a provider that finished loading before the group subscribed; a test
	// stretches it to isolate the wake-up a provider change gives.
	preWarmRecheck time.Duration

	testing      atomic.Bool
	lastQuick    atomic.TypedValue[time.Time]
	lastFull     atomic.TypedValue[time.Time]
	lastRecovery atomic.TypedValue[time.Time]
	hasFullCheck atomic.Bool
	// checkDone is closed when the check that holds `testing` ends, so an
	// explicit test joins a running check instead of sweeping the candidates a
	// second time, and checkResults is what that check measured alive.
	checkDone    chan struct{}
	checkResults map[string]uint16
}

// poolMeasurement is one emergency test of the last non-empty pool. A dial of
// an exhausted pool joins it: alive is closed once the first alive member is
// published, so the dial leaves on a measured member at once, and done is
// closed when the test ends, so a dial that has nothing else to try can take
// whatever the test found.
type poolMeasurement struct {
	alive    chan struct{}
	done     chan struct{}
	aliveHit bool
}

func newPoolMeasurement() *poolMeasurement {
	return &poolMeasurement{alive: make(chan struct{}), done: make(chan struct{})}
}

// DialContext implements C.ProxyAdapter
func (u *URLTestPool) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	u.maybeScheduleCheck()
	u.waitWarm(ctx)

	var proxy C.Proxy
	tried := make(map[string]struct{})

	for {
		candidate := u.nextCandidate(ctx, tried, false)
		if !candidate.ok {
			break
		}
		proxy = candidate.proxy
		tried[proxy.Name()] = struct{}{}

		dialCtx, cancel := u.attemptContext(ctx)
		c, err = proxy.DialContext(dialCtx, metadata)
		// The attempt context ended only by its own timeout, while the caller
		// still waits: the member did not connect within attempt-timeout.
		cut := ctx.Err() == nil && dialCtx.Err() != nil
		cancel()
		if err == nil {
			break
		}

		if cut {
			log.Debugln("ProxyGroup: %s: %s did not connect within %s", u.Name(), proxy.Name(), u.attemptTimeout)
		}
		u.onProxyFailed(proxy, err)

		if candidate.exhausted {
			if fallback := u.failedDialFallback(ctx, tried, candidate); fallback != nil {
				proxy = fallback
				tried[proxy.Name()] = struct{}{}
				c, err = proxy.DialContext(ctx, metadata)
				if err != nil {
					u.onProxyFailed(proxy, err)
				}
			}
			break
		}

		// Without attempt-timeout the pool keeps its single dial per call;
		// with it, the next member is tried while the caller's budget lasts.
		if u.attemptTimeout <= 0 || ctx.Err() != nil {
			break
		}
	}

	if err == nil {
		c.AppendToChains(u)
		u.noteDialSucceeded(proxy)
	}

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				u.noteDialSucceeded(proxy)
			} else {
				u.onProxyFailed(proxy, err)
			}
		})
	}

	return c, err
}

// attemptContext bounds one member dial by attempt-timeout without touching
// the caller's budget: a closer parent deadline still wins.
func (u *URLTestPool) attemptContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if u.attemptTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, u.attemptTimeout)
}

// ListenPacketContext implements C.ProxyAdapter
func (u *URLTestPool) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	u.maybeScheduleCheck()
	u.waitWarm(ctx)

	var lastErr error
	tried := make(map[string]struct{})

	for {
		candidate := u.nextCandidate(ctx, tried, true)
		if !candidate.ok {
			break
		}
		proxy := candidate.proxy
		tried[proxy.Name()] = struct{}{}

		pc, err := u.packetAttempt(ctx, proxy, metadata)
		if err == nil {
			return pc, nil
		}
		lastErr = err
		u.onProxyFailed(proxy, err)

		if candidate.exhausted {
			if fallback := u.failedDialFallback(ctx, tried, candidate); fallback != nil {
				proxy = fallback
				tried[proxy.Name()] = struct{}{}
				pc, err = u.packetAttempt(ctx, proxy, metadata)
				if err == nil {
					return pc, nil
				}
				lastErr = err
				u.onProxyFailed(proxy, err)
			}
			break
		}

		if u.attemptTimeout <= 0 || ctx.Err() != nil {
			break
		}
	}

	return nil, lastErr
}

// packetAttempt opens the packet flow of one member, reporting a member that
// did not answer within attempt-timeout while the caller still waited.
func (u *URLTestPool) packetAttempt(ctx context.Context, proxy C.Proxy, metadata *C.Metadata) (C.PacketConn, error) {
	dialCtx, cancel := u.attemptContext(ctx)
	pc, err := proxy.ListenPacketContext(dialCtx, metadata)
	cut := ctx.Err() == nil && dialCtx.Err() != nil
	cancel()
	if err != nil {
		if cut {
			log.Debugln("ProxyGroup: %s: %s did not connect within %s", u.Name(), proxy.Name(), u.attemptTimeout)
		}
		return nil, err
	}

	pc.AppendToChains(u)
	u.noteDialSucceeded(proxy)
	return pc, nil
}

// poolDialCandidate is one attempt of a traffic flow: the member to try and how
// the dial reacts to its failure.
type poolDialCandidate struct {
	proxy C.Proxy
	// exhausted marks a dial of a pool with nothing measured alive: its failure
	// is followed by the emergency test instead of the next pool member.
	exhausted bool
	// measured marks a member the emergency test found, not the blind
	// last-known-good fallback.
	measured bool
	// udp marks a packet flow: its fallbacks stay on the members that carry UDP.
	udp bool
	// ok reports whether there is a member to try at all.
	ok bool
}

// nextCandidate selects the member of the next attempt. A dial of an exhausted
// pool starts the emergency test of the last pool before the blind last-good
// dial and gives it a head start, so the connection leaves on a measured member
// as soon as one answers instead of paying for a failed last-good dial first.
func (u *URLTestPool) nextCandidate(ctx context.Context, tried map[string]struct{}, udp bool) poolDialCandidate {
	var proxy C.Proxy
	var exhausted bool
	if udp {
		proxy, exhausted = u.fastForPacket(true)
	} else {
		proxy, exhausted = u.fastForDial(true)
	}
	if _, ok := tried[proxy.Name()]; ok {
		// every fresh member has already been tried
		return poolDialCandidate{}
	}

	if exhausted {
		if measured := u.awaitRecovery(ctx, u.recoveryWait); measured != nil {
			if _, ok := tried[measured.Name()]; !ok && (!udp || measured.SupportUDP()) {
				log.Debugln("ProxyGroup: %s: leaving on %s measured by the emergency test", u.Name(), measured.Name())
				return poolDialCandidate{proxy: measured, exhausted: true, measured: true, udp: udp, ok: true}
			}
		}
		log.Debugln("ProxyGroup: %s: nothing measured alive, dialing %s", u.Name(), proxy.Name())
	}

	return poolDialCandidate{proxy: proxy, exhausted: exhausted, udp: udp, ok: true}
}

// failedDialFallback returns the member of the last attempt of an exhausted
// dial: the best member the emergency test reported, or the blind last-known-
// good dial when the attempt was the test's member and the test found nothing
// better. A packet flow keeps to the members that carry UDP.
func (u *URLTestPool) failedDialFallback(ctx context.Context, tried map[string]struct{}, candidate poolDialCandidate) C.Proxy {
	if !candidate.exhausted {
		return nil
	}

	if best := u.recoverAfterFailure(ctx); best != nil && (!candidate.udp || best.SupportUDP()) {
		if _, ok := tried[best.Name()]; !ok {
			return best
		}
	}

	if !candidate.measured {
		return nil
	}
	proxies := u.GetProxies(true)
	if candidate.udp {
		proxies = udpProxies(proxies)
	}
	if last := u.lastGoodProxy(proxies); last != nil {
		if _, ok := tried[last.Name()]; !ok {
			log.Debugln("ProxyGroup: %s: %s found by the emergency test failed, dialing the last known good %s", u.Name(), candidate.proxy.Name(), last.Name())
			return last
		}
	}
	return nil
}

// SupportUDP implements C.ProxyAdapter
func (u *URLTestPool) SupportUDP() bool {
	if u.disableUDP {
		return false
	}

	// The group carries UDP when any member does: the packet path picks its
	// member among them (fastForPacket), so a mixed pool keeps its packet flows
	// working instead of losing them whenever the fast member changes.
	for _, proxy := range u.GetProxies(false) {
		if proxy.SupportUDP() {
			return true
		}
	}
	return false
}

// IsL3Protocol implements C.ProxyAdapter
func (u *URLTestPool) IsL3Protocol(metadata *C.Metadata) bool {
	// The same "any member" rule as SupportUDP: an L3 member must not be handed
	// a domain, which is what the dns module decides by this answer.
	for _, proxy := range u.GetProxies(false) {
		if proxy.IsL3Protocol(metadata) {
			return true
		}
	}
	return false
}

// Unwrap implements C.ProxyAdapter
func (u *URLTestPool) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return u.fast(touch)
}

// Now implements ProxyGroup
func (u *URLTestPool) Now() string {
	return u.fast(false).Name()
}

// Providers implements ProxyGroup
func (u *URLTestPool) Providers() []P.ProxyProvider {
	return u.providers
}

// Proxies implements ProxyGroup
func (u *URLTestPool) Proxies() []C.Proxy {
	return u.GetProxies(false)
}

// Set implements SelectAble
func (u *URLTestPool) Set(name string) error {
	for _, proxy := range u.GetProxies(false) {
		if proxy.Name() == name {
			u.ForceSet(name)
			return nil
		}
	}

	return errors.New("proxy not exist")
}

// ForceSet implements SelectAble
func (u *URLTestPool) ForceSet(name string) {
	u.mu.Lock()
	u.selected = name
	u.fastNode = nil
	u.mu.Unlock()
}

// URLTest implements ProxyGroup
//
// A probe for the pool's own target is the pool's own full check: an explicit
// test measures the members with the timeout, the concurrency and the rebuild
// a scheduled check uses, so the pool it leaves cannot depend on which of the
// two ran last. The caller's context bounds only the answer; the sweep keeps
// going after it is gone, because the members still being probed are what the
// next connections need.
func (u *URLTestPool) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	if url != u.testUrl {
		return u.GroupBase.URLTest(ctx, url, expectedStatus)
	}

	done, _ := u.beginCheck(true)
	u.waitCheck(ctx, done)
	return u.lastResultsCopy(), nil
}

// PoolCheck measures only the members the pool holds and reports their delays.
// A parent group probes a candidate through its own check: asking the pool for
// its own view keeps the parent from sweeping every candidate of the providers
// and from rebuilding the pool from that partial result. A pool that holds no
// member to measure is swept on the parent's budget instead of waiting for the
// pool's own check, which the parent may not outlive.
func (u *URLTestPool) PoolCheck(ctx context.Context) (map[string]uint16, error) {
	u.mu.RLock()
	names := append([]string(nil), u.pool...)
	u.mu.RUnlock()

	if len(names) == 0 {
		return u.parentCheck(ctx)
	}

	proxies := u.GetProxies(true)
	members, complete := proxiesByNames(proxies, names)
	if !complete {
		// the proxy list has been changed, rebuild the pool
		return u.parentCheck(ctx)
	}

	results, tested := u.testProxies(ctx, members)
	// A member that failed leaves the pool now instead of holding a slot: the
	// parent asked precisely whether the pool can still carry traffic. A probe
	// the caller cut is not in the verdicts at all, so an interrupted check
	// cannot empty the pool.
	u.publishPartial(results, tested)
	if len(results) == 0 {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errNoAliveMember
	}

	u.mu.Lock()
	for name := range results {
		delete(u.suspicious, name)
	}
	u.mu.Unlock()
	u.lastQuick.Store(time.Now())
	return results, nil
}

// parentCheck sweeps every candidate on the caller's budget and rebuilds the
// pool from what it measured. It is the answer for a pool that holds no member
// to measure: the parent asks whether the pool can carry traffic at all, and it
// cannot outlive its own probe budget.
func (u *URLTestPool) parentCheck(ctx context.Context) (map[string]uint16, error) {
	results, err := u.GroupBase.URLTest(ctx, u.testUrl, u.expectedRanges)
	if err != nil {
		return results, err
	}
	u.applyResults(results)
	return results, nil
}

// AliveMember reports whether the pool still holds a member that can carry
// traffic, and whether that is known at all: a pool that has not finished a
// check yet is unknown, so a parent does not cut it on the first failure of a
// cold start. A member measured dead, or one that failed a dial since its last
// measurement, does not count: a pool whose every member failed answers
// "nothing left", and the parent switches to its next candidate.
func (u *URLTestPool) AliveMember() (alive, known bool) {
	if !u.hasFullCheck.Load() {
		return false, false
	}

	u.mu.RLock()
	names := make(map[string]struct{}, len(u.pool))
	for _, name := range u.pool {
		names[name] = struct{}{}
	}
	suspicious := make(map[string]struct{}, len(u.suspicious))
	now := time.Now()
	for name, until := range u.suspicious {
		if now.Before(until) {
			suspicious[name] = struct{}{}
		}
	}
	u.mu.RUnlock()

	if len(names) == 0 {
		return false, true
	}

	for _, proxy := range u.GetProxies(false) {
		if _, ok := names[proxy.Name()]; !ok {
			continue
		}
		if _, ok := suspicious[proxy.Name()]; ok {
			continue
		}
		if proxy.AliveForTestUrl(u.testUrl) {
			return true, true
		}
	}
	return false, true
}

// SelfTestTarget implements SelfTester: the URL and status the pool measures
// its members against.
func (u *URLTestPool) SelfTestTarget() (string, utils.IntRanges[uint16]) {
	return u.testUrl, u.expectedRanges
}

// MarshalJSON implements C.ProxyAdapter
func (u *URLTestPool) MarshalJSON() ([]byte, error) {
	all := make([]string, 0)
	for _, proxy := range u.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	now := u.Now()

	u.mu.RLock()
	pool := append([]string(nil), u.pool...)
	selected := u.selected
	lastGood := u.lastGood
	recovering := u.measuring != nil
	ts := time.Now()
	suspicious := make([]string, 0, len(u.suspicious))
	for name, until := range u.suspicious {
		if ts.Before(until) {
			suspicious = append(suspicious, name)
		}
	}
	u.mu.RUnlock()
	sort.Strings(suspicious)

	// The type is the adapter's own name: hosts that know url-test-pool show it
	// as such, and the built-in fields below still let the rest render the group
	// as a plain selector over the pool.
	return json.Marshal(map[string]any{
		"type":           C.URLTestPool.String(),
		"now":            now,
		"all":            all,
		"pool":           pool,
		"poolSize":       u.poolSize,
		"testUrl":        u.testUrl,
		"expectedStatus": u.expectedStatus,
		"fixed":          selected,
		"lastGood":       lastGood,
		"recovering":     recovering,
		"suspicious":     suspicious,
		"hidden":         u.Hidden(),
		"icon":           u.Icon(),
		"emptyFallback":  u.EmptyFallback().Name(),
	})
}

func (u *URLTestPool) fast(touch bool) C.Proxy {
	proxy, _ := u.fastForDial(touch)
	return proxy
}

// fastForDial returns the proxy to dial and reports whether it was chosen as a
// last-known-good fallback: the pool has no usable member, so the next failure
// should trigger an emergency test of the last pool instead of another blind dial.
func (u *URLTestPool) fastForDial(touch bool) (C.Proxy, bool) {
	return u.fastForTraffic(touch, nil)
}

// fastForPacket is fastForDial restricted to the members that carry UDP, so a
// packet flow of a mixed pool never lands on a member that drops it.
func (u *URLTestPool) fastForPacket(touch bool) (C.Proxy, bool) {
	return u.fastForTraffic(touch, udpProxies)
}

// udpProxies keeps the members that carry UDP. A list that filters down to
// nothing is left to the caller: the dial then reports the failure instead of
// silently leaving the group.
func udpProxies(proxies []C.Proxy) []C.Proxy {
	filtered := make([]C.Proxy, 0, len(proxies))
	for _, proxy := range proxies {
		if proxy.SupportUDP() {
			filtered = append(filtered, proxy)
		}
	}
	return filtered
}

func (u *URLTestPool) fastForTraffic(touch bool, usable func([]C.Proxy) []C.Proxy) (C.Proxy, bool) {
	// with lazy enabled the checks are driven only by real traffic,
	// but the very first observation always starts the initial full check
	if touch || !u.lazy || !u.hasFullCheck.Load() {
		u.maybeScheduleCheck()
	}

	proxies := u.GetProxies(touch)
	if usable != nil {
		if filtered := usable(proxies); len(filtered) > 0 {
			proxies = filtered
		}
	}
	if len(proxies) == 0 {
		return u.EmptyFallback(), false
	}

	proxy, exhausted := u.pickFast(proxies)
	if !exhausted {
		return proxy, false
	}

	if last := u.lastGoodProxy(proxies); last != nil {
		return last, true
	}

	u.requestFullCheckOnExhaustion()
	return proxy, true
}

// lastGoodProxy returns the proxy that carried the last successful dial, if it
// still exists in the current proxy list.
func (u *URLTestPool) lastGoodProxy(proxies []C.Proxy) C.Proxy {
	u.mu.RLock()
	name := u.lastGood
	u.mu.RUnlock()
	if name == "" {
		return nil
	}

	for _, proxy := range proxies {
		if proxy.Name() == name {
			return proxy
		}
	}
	return nil
}

func (u *URLTestPool) pickFast(proxies []C.Proxy) (C.Proxy, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.selected != "" {
		for _, proxy := range proxies {
			if proxy.Name() == u.selected && proxy.AliveForTestUrl(u.testUrl) && !u.isSuspiciousLocked(proxy.Name()) {
				u.fastNode = proxy
				return proxy, false
			}
		}
	}

	candidates := u.poolProxiesLocked(proxies)
	stale := len(u.pool) > 0 && len(candidates) == 0
	if len(candidates) == 0 {
		candidates = proxies
	}

	best := u.bestAliveLocked(candidates, true)
	exhausted := best == nil || stale
	if best == nil {
		// the pool is temporarily unusable, fall back to the rest of the proxies
		// while the full health check is being requested
		best = u.bestAliveLocked(candidates, false)
		if best == nil && len(candidates) != len(proxies) {
			best = u.bestAliveLocked(proxies, false)
		}
	}
	if best == nil {
		u.fastNode = nil
		return proxies[0], exhausted
	}

	if u.fastNode != nil && u.fastNode.AliveForTestUrl(u.testUrl) &&
		!u.isSuspiciousLocked(u.fastNode.Name()) && containsProxy(candidates, u.fastNode) &&
		u.fastNode.LastDelayForTestUrl(u.testUrl) <= best.LastDelayForTestUrl(u.testUrl)+u.tolerance {
		return u.fastNode, exhausted
	}

	u.fastNode = best
	return best, exhausted
}

func (u *URLTestPool) poolProxiesLocked(proxies []C.Proxy) []C.Proxy {
	pool, _ := proxiesByNames(proxies, u.pool)
	return pool
}

func (u *URLTestPool) bestAliveLocked(candidates []C.Proxy, skipSuspicious bool) C.Proxy {
	var best C.Proxy
	bestDelay := uint16(0xffff)
	for _, proxy := range candidates {
		if !proxy.AliveForTestUrl(u.testUrl) {
			continue
		}
		if skipSuspicious && u.isSuspiciousLocked(proxy.Name()) {
			continue
		}
		delay := proxy.LastDelayForTestUrl(u.testUrl)
		if best == nil || delay < bestDelay {
			best = proxy
			bestDelay = delay
		}
	}
	return best
}

func (u *URLTestPool) isSuspiciousLocked(name string) bool {
	until, ok := u.suspicious[name]
	return ok && time.Now().Before(until)
}

func (u *URLTestPool) onProxyFailed(proxy C.Proxy, err error) {
	if shouldTrackDialFailure(proxy.Type(), err) {
		u.markSuspicious(proxy.Name())
	}
	u.onDialFailed(proxy.Type(), err, u.healthCheck)
}

// healthCheck is called by GroupBase when the same proxy failed several times in a row
func (u *URLTestPool) healthCheck() {
	if last, ok := u.lastFull.LoadOk(); ok && time.Since(last) < u.interval {
		return
	}
	u.scheduleCheck(true)
}

func (u *URLTestPool) markSuspicious(name string) {
	u.mu.Lock()
	if u.suspicious == nil {
		u.suspicious = make(map[string]time.Time)
	}
	u.suspicious[name] = time.Now().Add(u.suspiciousTTL)
	if u.fastNode != nil && u.fastNode.Name() == name {
		u.fastNode = nil
	}

	exhausted := len(u.pool) > 0
	for _, poolName := range u.pool {
		if !u.isSuspiciousLocked(poolName) {
			exhausted = false
			break
		}
	}
	u.mu.Unlock()

	if exhausted {
		u.requestFullCheckOnExhaustion()
	}
}

func (u *URLTestPool) maybeScheduleCheck() {
	if !u.hasFullCheck.Load() {
		u.scheduleCheck(true)
		return
	}

	now := time.Now()
	if last, ok := u.lastFull.LoadOk(); !ok || now.Sub(last) >= u.fullInterval {
		u.scheduleCheck(true)
		return
	}
	if last, ok := u.lastQuick.LoadOk(); !ok || now.Sub(last) >= u.interval {
		u.scheduleCheck(false)
	}
}

// requestFullCheckOnExhaustion runs a full check when there is no usable proxy left.
// It is throttled by `interval`: after a full check that found nothing alive,
// and after a recent full check that found the pool alive but every dial still
// failed. The latter prevents a failed health check from probing the whole
// provider on every connection.
func (u *URLTestPool) requestFullCheckOnExhaustion() {
	if !u.shouldRequestFullCheckOnExhaustion(time.Now()) {
		return
	}
	u.scheduleCheck(true)
}

func (u *URLTestPool) shouldRequestFullCheckOnExhaustion(now time.Time) bool {
	u.mu.RLock()
	lastEmpty := u.lastEmptyFull
	empty := len(u.pool) == 0
	u.mu.RUnlock()

	if !lastEmpty.IsZero() && now.Sub(lastEmpty) < u.interval {
		return false
	}
	if !empty {
		if last, ok := u.lastFull.LoadOk(); ok && now.Sub(last) < u.interval {
			return false
		}
	}
	return true
}

func (u *URLTestPool) scheduleCheck(full bool) {
	u.beginCheck(full)
}

// beginCheck marks the pool as checking and returns the channel that is closed
// when the check ends. started reports whether this call runs it; a check that
// already holds the pool hands its own channel over, so an explicit test joins
// it instead of probing the same candidates again.
func (u *URLTestPool) beginCheck(full bool) (done <-chan struct{}, started bool) {
	if !u.testing.CompareAndSwap(false, true) {
		u.mu.RLock()
		done = u.checkDone
		u.mu.RUnlock()
		return done, false
	}

	channel := make(chan struct{})
	u.mu.Lock()
	u.checkDone = channel
	u.checkResults = nil
	u.mu.Unlock()

	if full {
		u.beginWarmup()
	}
	go func() {
		defer func() {
			u.testing.Store(false)
			u.mu.Lock()
			if u.checkDone == channel {
				u.checkDone = nil
			}
			u.mu.Unlock()
			close(channel)
		}()
		defer u.markWarm()
		if full {
			u.fullCheck()
		} else {
			u.quickCheck()
		}
	}()
	return channel, true
}

// waitCheck waits for the check to end or the caller to give up. A nil channel
// means the running check has not registered itself yet: the caller takes what
// is measured right now instead of waiting for a check it cannot see.
func (u *URLTestPool) waitCheck(ctx context.Context, done <-chan struct{}) {
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// storeResults keeps what a check measured, so an explicit test that joined it
// answers with the numbers the check published.
func (u *URLTestPool) storeResults(results map[string]uint16) {
	u.mu.Lock()
	u.checkResults = results
	u.mu.Unlock()
}

// lastResultsCopy reports what the last check measured alive.
func (u *URLTestPool) lastResultsCopy() map[string]uint16 {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if len(u.checkResults) == 0 {
		return nil
	}

	results := make(map[string]uint16, len(u.checkResults))
	for name, delay := range u.checkResults {
		results[name] = delay
	}
	return results
}

// beginWarmup opens the channel a cold-start dial waits on until the first
// measured member is published.
func (u *URLTestPool) beginWarmup() {
	u.mu.Lock()
	if u.warmCh == nil {
		u.warmCh = make(chan struct{})
		u.checkStart = time.Now()
	}
	u.mu.Unlock()
}

// markWarm releases the dials waiting for the first result of the check.
func (u *URLTestPool) markWarm() {
	u.mu.Lock()
	if u.warmCh != nil {
		close(u.warmCh)
		u.warmCh = nil
	}
	u.mu.Unlock()
}

// waitWarm lets dials that arrive before the first measured member wait for it,
// so a connection is not routed to the first proxy of the list only because
// nothing has been measured yet. The wait is capped by the probe timeout (a
// result cannot arrive sooner than one probe) and counted from the start of the
// check, so the whole check cycle delays dials by at most firstResultWait. It
// also never spends the retry reserve of the caller's budget.
func (u *URLTestPool) waitWarm(ctx context.Context) {
	if u.firstResultWait <= 0 {
		return
	}

	u.mu.RLock()
	cold := len(u.pool) == 0
	channel := u.warmCh
	started := u.checkStart
	u.mu.RUnlock()
	if !cold || channel == nil {
		return
	}

	wait := u.firstResultWait - time.Since(started)
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline) - urlTestPoolRetryReserve; remaining < wait {
			wait = remaining
		}
	}
	if wait <= 0 {
		return
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-channel:
	case <-timer.C:
	case <-ctx.Done():
	}
}

// WarmUp fills the pool before the first dial arrives. url-test and fallback
// get that measurement from the health check of their provider; a pool keeps
// its own schedule, which the provider ticker must not run, so the executor
// asks it directly.
//
// The group exists before the providers of an embedded core finish loading, so
// the check waits for them: a full check started over the empty fallback would
// only mark the pool empty for the next interval. A change of one of the
// group's providers wakes the wait, which turns it into one proxy list read
// per change instead of a poll every interval. `lazy` opts out, because the
// profile asked for checks driven only by real traffic.
func (u *URLTestPool) WarmUp() {
	if u.lazy {
		return
	}
	if !u.preWarming.CompareAndSwap(false, true) {
		return
	}

	// Only a change of one of the group's own providers can fill the pool, so a
	// change of any other provider costs nothing.
	own := make(map[string]struct{}, len(u.providers))
	for _, provider := range u.providers {
		own[provider.Name()] = struct{}{}
	}

	loaded := make(chan struct{}, 1)
	cancel := P.OnChange(func(name string) {
		if _, ok := own[name]; !ok {
			return
		}
		select {
		case loaded <- struct{}{}:
		default:
		}
	})

	go func() {
		defer cancel()
		if !u.waitProvidersReady(urlTestPoolPreWarmWait, loaded) {
			return
		}
		u.maybeScheduleCheck()
	}()
}

// waitProvidersReady waits until the providers of the group have proxies and
// reports whether they arrived within the given wait. It reads the proxy list
// without touching the providers, so the wait never starts a check of its own,
// and it re-reads it on a provider change rather than on a poll.
func (u *URLTestPool) waitProvidersReady(wait time.Duration, loaded <-chan struct{}) bool {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	recheckInterval := u.preWarmRecheck
	if recheckInterval <= 0 {
		recheckInterval = urlTestPoolPreWarmRecheck
	}
	recheck := time.NewTicker(recheckInterval)
	defer recheck.Stop()

	for {
		if u.proxiesReady(u.GetProxies(false)) {
			return true
		}
		select {
		case <-loaded:
		case <-recheck.C:
		case <-deadline.C:
			return u.proxiesReady(u.GetProxies(false))
		}
	}
}

func (u *URLTestPool) quickCheck() {
	u.mu.RLock()
	names := append([]string(nil), u.pool...)
	u.mu.RUnlock()

	if len(names) == 0 {
		u.fullCheck()
		return
	}

	proxies := u.GetProxies(true)
	poolProxies, complete := proxiesByNames(proxies, names)
	if !complete {
		// the proxy list has been changed, rebuild the pool
		u.fullCheck()
		return
	}

	log.Debugln("ProxyGroup: %s start quick health check for %d proxies", u.Name(), len(poolProxies))
	// Every result is published as it arrives, so a member that just failed
	// leaves the pool now instead of holding its slot until the next full
	// check, and a member that answered is usable before the slow ones end.
	results, _ := u.testProxiesStreaming(context.Background(), poolProxies)
	u.storeResults(results)
	u.lastQuick.Store(time.Now())

	if len(results) == 0 {
		log.Debugln("ProxyGroup: %s all pool proxies are unavailable, start full health check", u.Name())
		u.fullCheck()
		return
	}

	u.mu.Lock()
	for name := range results {
		delete(u.suspicious, name)
	}
	u.mu.Unlock()

	if u.shouldRunFullCheck(len(results), u.poolTarget(len(proxies))) {
		log.Debugln("ProxyGroup: %s pool is underfilled for %d quick checks, start full health check", u.Name(), u.degradedChecks)
		u.fullCheck()
	}
}

// poolTarget is how many alive members the pool should hold: its configured
// size, capped by how many proxies exist, so a provider smaller than the pool
// does not look underfilled.
func (u *URLTestPool) poolTarget(available int) int {
	if available < u.poolSize {
		return available
	}
	return u.poolSize
}

// shouldRunFullCheck counts consecutive quick checks with an underfilled pool
// and reports whether a full check should rebuild the pool. The alive count is
// compared with the target size rather than the current pool length: a member
// that just failed has already left the pool, so comparing with it would never
// see the pool as underfilled.
func (u *URLTestPool) shouldRunFullCheck(alive, target int) bool {
	if u.degradedChecks <= 0 || target <= 0 {
		return false
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	if alive >= target {
		u.degradedCount = 0
		return false
	}

	u.degradedCount++
	if u.degradedCount < u.degradedChecks {
		return false
	}
	u.degradedCount = 0
	return true
}

// noteDialSucceeded remembers the proxy that proved connectivity and rebuilds
// an empty pool (for example after a full outage). The check runs in
// background and is throttled, so dials never wait for it.
func (u *URLTestPool) noteDialSucceeded(proxy C.Proxy) {
	u.mu.Lock()
	u.lastGood = proxy.Name()
	empty := len(u.pool) == 0
	u.mu.Unlock()
	if !empty {
		return
	}

	if last, ok := u.lastFull.LoadOk(); ok && time.Since(last) < urlTestPoolRecoveryCooldown {
		return
	}
	u.scheduleCheck(true)
}

// recoverAfterFailure joins the emergency test of the last non-empty pool and
// waits for its outcome: the first alive member as soon as its probe answers,
// or the end of the test. A dial that has already failed the last-good fallback
// has nothing better to do, and the wait leaves the retry reserve of the tunnel
// untouched.
func (u *URLTestPool) recoverAfterFailure(ctx context.Context) C.Proxy {
	if !u.ensureRecovery(ctx) {
		return nil
	}

	u.mu.RLock()
	measure := u.measuring
	u.mu.RUnlock()
	if measure == nil {
		return u.bestMeasuredAlive()
	}

	timeout, bounded := recoveryWaitLeft(ctx)
	if bounded && timeout <= 0 {
		return u.bestMeasuredAlive()
	}
	var expire <-chan time.Time
	if bounded {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		expire = timer.C
	}

	select {
	case <-measure.alive:
	case <-measure.done:
	case <-expire:
	case <-ctx.Done():
	}
	return u.bestMeasuredAlive()
}

// ensureRecovery starts the emergency test of the last non-empty pool when none
// is running, and reports whether a test is running at all. It is single-flight
// and throttled, so an ongoing outage cannot turn every dial into a burst of
// probes; a dial that arrives while a test runs joins it.
func (u *URLTestPool) ensureRecovery(ctx context.Context) bool {
	u.mu.RLock()
	running := u.measuring != nil
	names := append([]string(nil), u.lastPool...)
	u.mu.RUnlock()
	if running {
		return true
	}

	if !u.recoveryStartAllowed(ctx) {
		return false
	}
	if last, ok := u.lastRecovery.LoadOk(); ok && time.Since(last) < urlTestPoolRecoveryTestInterval {
		return false
	}

	if len(names) == 0 {
		u.requestFullCheckOnExhaustion()
		return false
	}

	proxies := u.GetProxies(true)
	candidates, _ := proxiesByNames(proxies, names)
	if len(candidates) == 0 {
		u.requestFullCheckOnExhaustion()
		return false
	}

	measure := newPoolMeasurement()
	u.mu.Lock()
	if u.measuring != nil {
		u.mu.Unlock()
		return true
	}
	u.measuring = measure
	u.mu.Unlock()

	u.lastRecovery.Store(time.Now())
	log.Debugln("ProxyGroup: %s: no member measured alive, testing %d members of the last pool", u.Name(), len(candidates))

	// The test runs on its own context: a dial that leaves on its first result
	// must not cancel the rest, the members still being probed are what the
	// next connection needs.
	go func() {
		// The probe timeout is the recovery one, never what the starting dial
		// has left: every later dial joins this test, and a short budget of one
		// caller must not turn a slow member into a dead one.
		results, tested := u.probeProxies(context.Background(), candidates, urlTestPoolRecoveryTestTimeout, true)
		u.applyRecovery(results, tested, len(candidates))
		u.finishMeasurement(measure)
	}()
	return true
}

// awaitRecovery starts or joins the emergency test and waits at most `wait` for
// the first member it finds alive. A zero wait only accepts a member that is
// already measured, which is how an exhausted dial decides between a measured
// member and the last-good fallback.
func (u *URLTestPool) awaitRecovery(ctx context.Context, wait time.Duration) C.Proxy {
	if !u.ensureRecovery(ctx) {
		return nil
	}

	u.mu.RLock()
	measure := u.measuring
	u.mu.RUnlock()
	if measure == nil {
		return u.bestMeasuredAlive()
	}
	if best := u.bestMeasuredAlive(); best != nil {
		return best
	}

	if left, bounded := recoveryWaitLeft(ctx); bounded && left < wait {
		wait = left
	}
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-measure.alive:
	case <-measure.done:
	case <-timer.C:
	case <-ctx.Done():
	}
	return u.bestMeasuredAlive()
}

// recoveryStartAllowed reports whether the caller still has enough budget left
// to leave on the result of an emergency test. The test itself always probes
// with the recovery timeout: it is shared with every dial that joins it, so it
// must not inherit the leftover budget of the dial that started it.
func (u *URLTestPool) recoveryStartAllowed(ctx context.Context) bool {
	left, bounded := recoveryWaitLeft(ctx)
	return !bounded || left > 0
}

// recoveryWaitLeft returns how long the caller can still wait for an emergency
// test while keeping its retry reserve, and whether its deadline bounds that
// wait at all.
func recoveryWaitLeft(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return time.Until(deadline) - urlTestPoolRetryReserve, true
}

// bestMeasuredAlive returns the fastest member the pool can use right now, as
// published by the emergency test. It is the member a dial of an exhausted pool
// leaves on.
func (u *URLTestPool) bestMeasuredAlive() C.Proxy {
	proxies := u.GetProxies(false)

	u.mu.RLock()
	defer u.mu.RUnlock()

	candidates := u.poolProxiesLocked(proxies)
	if len(candidates) == 0 {
		candidates = proxies
	}
	return u.bestAliveLocked(candidates, true)
}

// finishMeasurement closes the emergency test of the pool: a dial still waiting
// for its outcome wakes up and takes whatever the test found.
func (u *URLTestPool) finishMeasurement(measure *poolMeasurement) {
	u.mu.Lock()
	if u.measuring == measure {
		u.measuring = nil
	}
	u.mu.Unlock()
	close(measure.done)
}

// applyRecovery publishes an emergency test without consuming the periodic
// schedule: the pool is rebuilt from what is alive right now, but a full check
// that is due stays due. Recording the test as a full check would postpone the
// sweep of every candidate by the full interval, and that sweep is what leaves
// the members of a network that is gone behind.
func (u *URLTestPool) applyRecovery(results map[string]uint16, tested map[string]struct{}, candidates int) {
	u.publishPool(results, tested, false)
	u.lastQuick.Store(time.Now())
	// Only a sweep that reached every candidate is a verdict about the pool: a
	// test that walked all of them and found nothing may tell a parent it has
	// nothing left, a partial one must not.
	if len(results) > 0 || len(tested) >= candidates {
		u.hasFullCheck.Store(true)
	}
	u.markWarm()
}

func (u *URLTestPool) fullCheck() {
	proxies := u.GetProxies(true)
	if !u.proxiesReady(proxies) {
		return
	}

	log.Debugln("ProxyGroup: %s start full health check for %d proxies", u.Name(), len(proxies))
	// Publish every measured member as it arrives, so a cold pool routes by a
	// real delay from the first result instead of waiting for the whole list.
	results, _ := u.testProxiesStreaming(context.Background(), proxies)
	u.storeResults(results)
	u.applyResults(results)
}

func (u *URLTestPool) applyResults(results map[string]uint16) {
	u.publishPool(results, nil, true)

	now := time.Now()
	u.lastFull.Store(now)
	u.lastQuick.Store(now)
	u.hasFullCheck.Store(true)
	u.markWarm()
}

// publishPartial rebuilds the pool from the results collected so far, so a dial
// that arrives during a full check already routes by a measured delay instead
// of the first proxy of the list. It runs for every finished probe, so the pool
// reaches its full size while the slow members are still being probed and a
// member that failed leaves it at once. The check bookkeeping is left to
// applyResults.
func (u *URLTestPool) publishPartial(results map[string]uint16, tested map[string]struct{}) {
	u.publishPool(results, tested, false)
	u.markWarm()
}

// publishPool replaces the pool with the fastest measured proxies. A partial
// result set is published as soon as it arrives, so a dial during a check
// already routes by a measured delay instead of the first proxy of the list;
// the members that have not been measured yet are kept after it, so the pool a
// dial can choose from never shrinks while the check is running. A member that
// was measured and failed is not kept: it is known to be unavailable, and it
// must not hold a slot until the next full check. Only a final result set
// updates the emergency recovery set and the empty marker, because the recovery
// check replays lastPool and the marker throttles the next one.
func (u *URLTestPool) publishPool(results map[string]uint16, tested map[string]struct{}, final bool) {
	names := sortedProxyNames(results)
	if len(names) > u.poolSize {
		names = names[:u.poolSize]
	}

	now := time.Now()
	u.mu.Lock()
	if !final {
		for _, name := range u.pool {
			if len(names) >= u.poolSize {
				break
			}
			if _, measured := results[name]; measured {
				continue
			}
			if _, failed := tested[name]; failed {
				continue
			}
			names = append(names, name)
		}
	}
	u.pool = names
	if final && len(names) > 0 {
		u.lastPool = append([]string(nil), names...)
	}
	for name := range results {
		delete(u.suspicious, name)
	}
	if measure := u.measuring; measure != nil && !measure.aliveHit && len(results) > 0 {
		// A dial of the exhausted pool waits for a member it can leave on, and
		// results holds only measured alive members: whichever check measured
		// one, the pool now has it.
		measure.aliveHit = true
		close(measure.alive)
	}
	if final {
		// a full result set rebuilds the pool from every candidate, so the
		// underfilled streak starts over
		u.degradedCount = 0
		if len(names) == 0 {
			u.lastEmptyFull = now
		} else {
			u.lastEmptyFull = time.Time{}
		}
	}
	u.mu.Unlock()
}

// testProxies tests proxies concurrently and returns delay (ms) of alive proxies
// together with every proxy that was actually measured, alive or not, so the
// pool can tell a failed member from one the check did not reach.
func (u *URLTestPool) testProxies(ctx context.Context, proxies []C.Proxy) (map[string]uint16, map[string]struct{}) {
	return u.probeProxies(ctx, proxies, time.Duration(u.testTimeout)*time.Millisecond, false)
}

// testProxiesStreaming is testProxies that publishes every measured member as
// soon as its probe returns, so the pool is usable before the whole list has
// been probed and a cold start routes by the first real result.
func (u *URLTestPool) testProxiesStreaming(ctx context.Context, proxies []C.Proxy) (map[string]uint16, map[string]struct{}) {
	return u.probeProxies(ctx, proxies, time.Duration(u.testTimeout)*time.Millisecond, true)
}

func (u *URLTestPool) probeProxies(ctx context.Context, proxies []C.Proxy, timeout time.Duration, publish bool) (map[string]uint16, map[string]struct{}) {
	results := make(map[string]uint16, len(proxies))
	tested := make(map[string]struct{}, len(proxies))
	var mu sync.Mutex

	group := new(errgroup.Group)
	group.SetLimit(urlTestPoolCheckLimit)
	for _, proxy := range proxies {
		proxy := proxy
		group.Go(func() error {
			// a caller that gives up (a parent's probe budget, a cascade that
			// moved on) ends its probes too instead of keeping them running
			probeCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			delay, err := proxy.URLTest(probeCtx, u.testUrl, u.expectedRanges)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if ctx.Err() != nil {
					// the caller gave up, so the probe was cut instead of
					// failing: that is not a verdict about the member
					return nil
				}
				tested[proxy.Name()] = struct{}{}
				if publish {
					// the failed member leaves the pool now instead of
					// holding a slot until the check ends
					u.publishPartial(results, tested)
				}
				return nil
			}

			tested[proxy.Name()] = struct{}{}
			results[proxy.Name()] = delay
			if publish {
				// published under the results lock: a later publish always
				// carries every earlier result, so the pool cannot shrink back
				u.publishPartial(results, tested)
			}
			return nil
		})
	}
	_ = group.Wait()

	return results, tested
}

// proxiesReady reports whether the providers already loaded their proxies,
// otherwise a full check would rebuild the pool from the empty fallback.
func (u *URLTestPool) proxiesReady(proxies []C.Proxy) bool {
	return len(proxies) != 1 || proxies[0] != u.EmptyFallback()
}

func sortedProxyNames(results map[string]uint16) []string {
	names := make([]string, 0, len(results))
	for name := range results {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool {
		if results[names[i]] != results[names[j]] {
			return results[names[i]] < results[names[j]]
		}
		return names[i] < names[j]
	})
	return names
}

func containsProxy(proxies []C.Proxy, target C.Proxy) bool {
	for _, proxy := range proxies {
		if proxy == target {
			return true
		}
	}
	return false
}

// proxiesByNames resolves the given member names against the current proxy
// list, preserving the requested order and reporting whether every name was
// found. It runs on the dial path, so it allocates a set of the requested
// names instead of a lookup map over every member of the group.
func proxiesByNames(proxies []C.Proxy, names []string) ([]C.Proxy, bool) {
	if len(names) == 0 {
		return nil, true
	}

	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}

	found := make(map[string]C.Proxy, len(names))
	for _, proxy := range proxies {
		if _, ok := wanted[proxy.Name()]; ok {
			found[proxy.Name()] = proxy
		}
	}

	resolved := make([]C.Proxy, 0, len(names))
	for _, name := range names {
		if proxy, ok := found[name]; ok {
			resolved = append(resolved, proxy)
		}
	}
	return resolved, len(resolved) == len(names)
}

func shouldTrackDialFailure(adapterType C.AdapterType, err error) bool {
	switch adapterType {
	case C.Direct, C.Compatible, C.Reject, C.Pass, C.RejectDrop:
		return false
	}
	// A cancelled dial means the caller gave up on the request, which says
	// nothing about the member: only the proxy's own failure may cost it its
	// place in the pool.
	if errors.Is(err, context.Canceled) {
		return false
	}
	return !errors.Is(err, C.ErrNotSupport)
}

func NewURLTestPool(option GroupCommonOption, poolOption URLTestPoolOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*URLTestPool, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}

	for _, provider := range providers {
		for _, proxy := range provider.Proxies() {
			if _, ok := proxy.Adapter().(ProxyGroup); ok {
				return nil, fmt.Errorf("url-test-pool can't contain proxy group: %s", proxy.Name())
			}
		}
	}

	expectedStatus, err := utils.NewUnsignedRanges[uint16](option.ExpectedStatus)
	if err != nil {
		return nil, err
	}

	interval := time.Duration(option.Interval) * time.Second
	if interval <= 0 {
		interval = defaultURLTestPoolInterval
	}
	fullInterval := time.Duration(poolOption.FullInterval) * time.Second
	if fullInterval <= 0 {
		fullInterval = defaultURLTestPoolFullInterval
	}
	if fullInterval < interval {
		return nil, fmt.Errorf("full-interval %s must be greater than or equal to interval %s", fullInterval, interval)
	}

	poolSize := poolOption.PoolSize
	if poolSize <= 0 {
		poolSize = defaultURLTestPoolSize
	}

	degradedChecks := defaultURLTestPoolDegradedChecks
	if poolOption.DegradedChecks != nil {
		degradedChecks = *poolOption.DegradedChecks
		if degradedChecks < 0 {
			degradedChecks = 0
		}
	}

	tolerance := defaultURLTestTolerance
	if poolOption.Tolerance != nil {
		tolerance = *poolOption.Tolerance
	}

	// The tunnel owns the dial budget of every group, so these keys no longer
	// change anything. A profile that still sets them is told instead of being
	// silently ignored by the decoder.
	if poolOption.DialTimeout != nil || poolOption.DialRetryLimit != nil {
		log.Warnln("ProxyGroup: %s ignores dial-timeout/dial-retry-limit, the tunnel applies its own dial budget", option.Name)
	}

	attemptTimeout := time.Duration(0)
	if poolOption.AttemptTimeout != nil {
		attemptTimeout = time.Duration(*poolOption.AttemptTimeout) * time.Millisecond
		if attemptTimeout < 0 {
			attemptTimeout = 0
		}
	}

	testUrl := option.URL
	if testUrl == "" {
		testUrl = C.DefaultTestURL
	}

	base := NewGroupBase(GroupBaseOption{
		Name:           option.Name,
		Type:           C.URLTestPool,
		Hidden:         option.Hidden,
		Icon:           option.Icon,
		Filter:         option.Filter,
		ExcludeFilter:  option.ExcludeFilter,
		ExcludeType:    option.ExcludeType,
		TestTimeout:    option.TestTimeout,
		MaxFailedTimes: option.MaxFailedTimes,
		EmptyFallback:  emptyFallback,
		Providers:      providers,
	})

	// A result cannot be published before one probe finishes, so the probe
	// timeout is the meaningful cap on the wait of a cold start.
	firstResultWait := time.Duration(base.testTimeout) * time.Millisecond
	if firstResultWait > defaultURLTestPoolFirstResultWait {
		firstResultWait = defaultURLTestPoolFirstResultWait
	}

	return &URLTestPool{
		GroupBase:       base,
		disableUDP:      option.DisableUDP,
		testUrl:         testUrl,
		expectedStatus:  option.ExpectedStatus,
		expectedRanges:  expectedStatus,
		interval:        interval,
		fullInterval:    fullInterval,
		poolSize:        poolSize,
		degradedChecks:  degradedChecks,
		tolerance:       tolerance,
		firstResultWait: firstResultWait,
		attemptTimeout:  attemptTimeout,
		recoveryWait:    urlTestPoolRecoveryWait,
		lazy:            option.Lazy,
		preWarmRecheck:  urlTestPoolPreWarmRecheck,
		// a member that failed a dial is skipped until the next quick check
		// could have re-measured it
		suspiciousTTL: interval,
		suspicious:    make(map[string]time.Time),
	}, nil
}
