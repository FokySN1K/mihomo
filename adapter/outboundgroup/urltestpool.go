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
	// defaultURLTestPoolFirstResultWait caps how long a dial of a cold start
	// waits for the first measured member. The wait is also bounded by the probe
	// timeout (a result cannot arrive sooner) and is counted from the start of
	// the check, so a check cycle never delays dials longer than this.
	defaultURLTestPoolFirstResultWait = 2 * time.Second
	// urlTestPoolPreWarmPoll is how often a pre-warm looks for the proxies of
	// the group: an embedded core builds the group before its providers finish
	// loading, so the proxies may arrive after the config is applied.
	urlTestPoolPreWarmPoll = 100 * time.Millisecond
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
	lazy            bool

	mu            sync.RWMutex
	selected      string
	pool          []string
	lastPool      []string
	lastGood      string
	suspicious    map[string]struct{}
	fastNode      C.Proxy
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

	testing      atomic.Bool
	recovering   atomic.Bool
	lastQuick    atomic.TypedValue[time.Time]
	lastFull     atomic.TypedValue[time.Time]
	lastRecovery atomic.TypedValue[time.Time]
	hasFullCheck atomic.Bool
}

// DialContext implements C.ProxyAdapter
func (u *URLTestPool) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	u.maybeScheduleCheck()
	u.waitWarm(ctx)

	var proxy C.Proxy
	tried := make(map[string]struct{})

	for {
		var recovery bool
		proxy, recovery = u.fastForDial(true)
		if _, ok := tried[proxy.Name()]; ok {
			// every fresh member has already been tried
			break
		}
		tried[proxy.Name()] = struct{}{}

		dialCtx, cancel := u.attemptContext(ctx)
		c, err = proxy.DialContext(dialCtx, metadata)
		cancel()
		if err == nil {
			break
		}

		u.onProxyFailed(proxy, err)

		if recovery {
			if fallback := u.recoverAfterFailure(ctx); fallback != nil {
				proxy = fallback
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
		proxy, _ := u.fastForDial(true)
		if _, ok := tried[proxy.Name()]; ok {
			break
		}
		tried[proxy.Name()] = struct{}{}

		dialCtx, cancel := u.attemptContext(ctx)
		pc, err := proxy.ListenPacketContext(dialCtx, metadata)
		cancel()
		if err == nil {
			pc.AppendToChains(u)
			u.noteDialSucceeded(proxy)
			return pc, nil
		}

		lastErr = err
		u.onProxyFailed(proxy, err)
		if u.attemptTimeout <= 0 || ctx.Err() != nil {
			break
		}
	}

	return nil, lastErr
}

// SupportUDP implements C.ProxyAdapter
func (u *URLTestPool) SupportUDP() bool {
	if u.disableUDP {
		return false
	}

	return u.fast(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (u *URLTestPool) IsL3Protocol(metadata *C.Metadata) bool {
	return u.fast(false).IsL3Protocol(metadata)
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
func (u *URLTestPool) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	mp, err := u.GroupBase.URLTest(ctx, url, expectedStatus)
	if err == nil && url == u.testUrl {
		u.applyResults(mp)
	}
	return mp, err
}

// PoolCheck measures only the members the pool holds and reports their delays.
// A parent group probes a candidate through its own check: asking the pool for
// its own view keeps the parent from sweeping every candidate of the providers
// and from rebuilding the pool from that partial result. An empty pool has
// nothing to measure, so a full check fills it, which is also what the pool's
// own schedule would do.
func (u *URLTestPool) PoolCheck(ctx context.Context) (map[string]uint16, error) {
	u.mu.RLock()
	names := append([]string(nil), u.pool...)
	u.mu.RUnlock()

	if len(names) == 0 {
		return u.URLTest(ctx, u.testUrl, u.expectedRanges)
	}

	proxies := u.GetProxies(true)
	members, complete := proxiesByNames(proxies, names)
	if !complete {
		// the proxy list has been changed, rebuild the pool
		return u.URLTest(ctx, u.testUrl, u.expectedRanges)
	}

	results, tested := u.testProxies(members)
	// A member that failed leaves the pool now instead of holding a slot: the
	// parent asked precisely whether the pool can still carry traffic.
	u.publishPartial(results, tested)
	if len(results) == 0 {
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
	for name := range u.suspicious {
		suspicious[name] = struct{}{}
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
	u.mu.RUnlock()

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
	// with lazy enabled the checks are driven only by real traffic,
	// but the very first observation always starts the initial full check
	if touch || !u.lazy || !u.hasFullCheck.Load() {
		u.maybeScheduleCheck()
	}

	proxies := u.GetProxies(touch)
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
	_, ok := u.suspicious[name]
	return ok
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
		u.suspicious = make(map[string]struct{})
	}
	u.suspicious[name] = struct{}{}
	if u.fastNode != nil && u.fastNode.Name() == name {
		u.fastNode = nil
	}

	exhausted := len(u.pool) > 0
	for _, poolName := range u.pool {
		if _, ok := u.suspicious[poolName]; !ok {
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
	if !u.testing.CompareAndSwap(false, true) {
		return
	}
	if full {
		u.beginWarmup()
	}
	go func() {
		defer u.testing.Store(false)
		defer u.markWarm()
		if full {
			u.fullCheck()
		} else {
			u.quickCheck()
		}
	}()
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
// only mark the pool empty for the next interval. `lazy` opts out, because the
// profile asked for checks driven only by real traffic.
func (u *URLTestPool) WarmUp() {
	if u.lazy {
		return
	}
	if !u.preWarming.CompareAndSwap(false, true) {
		return
	}
	go func() {
		if !u.waitProvidersReady(urlTestPoolPreWarmWait) {
			return
		}
		u.maybeScheduleCheck()
	}()
}

// waitProvidersReady waits until the providers of the group have proxies and
// reports whether they arrived within the given wait. It reads the proxy list
// without touching the providers, so the wait never starts a check of its own.
func (u *URLTestPool) waitProvidersReady(wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for {
		if u.proxiesReady(u.GetProxies(false)) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(urlTestPoolPreWarmPoll)
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
	results, tested := u.testProxies(poolProxies)
	u.lastQuick.Store(time.Now())

	if len(results) == 0 {
		log.Debugln("ProxyGroup: %s all pool proxies are unavailable, start full health check", u.Name())
		u.fullCheck()
		return
	}

	// Republish so a member that just failed leaves the pool now instead of
	// holding its slot until the next full check; the members that were not
	// measured keep their place after the measured ones.
	u.publishPartial(results, tested)

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

// recoverAfterFailure runs an emergency test of the last non-empty pool after
// the last-good proxy failed. It is single-flight and throttled, so an ongoing
// outage cannot turn every dial into a burst of probes.
func (u *URLTestPool) recoverAfterFailure(ctx context.Context) C.Proxy {
	if last, ok := u.lastRecovery.LoadOk(); ok && time.Since(last) < urlTestPoolRecoveryTestInterval {
		return nil
	}
	if !u.recovering.CompareAndSwap(false, true) {
		return nil
	}
	defer u.recovering.Store(false)

	timeout := urlTestPoolRecoveryTestTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline) - urlTestPoolRetryReserve
		if remaining <= 0 {
			return nil
		}
		if remaining < timeout {
			timeout = remaining
		}
	}

	u.mu.RLock()
	names := append([]string(nil), u.lastPool...)
	u.mu.RUnlock()
	if len(names) == 0 {
		u.requestFullCheckOnExhaustion()
		return nil
	}

	proxies := u.GetProxies(true)
	candidates, _ := proxiesByNames(proxies, names)
	if len(candidates) == 0 {
		u.requestFullCheckOnExhaustion()
		return nil
	}

	u.lastRecovery.Store(time.Now())
	results, _ := u.testProxiesWithTimeout(candidates, timeout)
	if len(results) == 0 {
		return nil
	}
	u.applyResults(results)

	u.mu.RLock()
	best := ""
	if len(u.pool) > 0 {
		best = u.pool[0]
	}
	u.mu.RUnlock()
	if best == "" {
		return nil
	}

	return proxyByName(proxies, best)
}

func (u *URLTestPool) fullCheck() {
	proxies := u.GetProxies(true)
	if !u.proxiesReady(proxies) {
		return
	}

	log.Debugln("ProxyGroup: %s start full health check for %d proxies", u.Name(), len(proxies))
	// Publish every measured member as it arrives, so a cold pool routes by a
	// real delay from the first result instead of waiting for the whole list.
	results, _ := u.testProxiesStreaming(proxies)
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
func (u *URLTestPool) testProxies(proxies []C.Proxy) (map[string]uint16, map[string]struct{}) {
	return u.probeProxies(proxies, time.Duration(u.testTimeout)*time.Millisecond, false)
}

// testProxiesStreaming is testProxies that publishes every measured member as
// soon as its probe returns, so the pool is usable before the whole list has
// been probed and a cold start routes by the first real result.
func (u *URLTestPool) testProxiesStreaming(proxies []C.Proxy) (map[string]uint16, map[string]struct{}) {
	return u.probeProxies(proxies, time.Duration(u.testTimeout)*time.Millisecond, true)
}

// testProxiesWithTimeout is testProxies with a custom timeout, used by the
// emergency recovery check to stay inside the connection deadline.
func (u *URLTestPool) testProxiesWithTimeout(proxies []C.Proxy, timeout time.Duration) (map[string]uint16, map[string]struct{}) {
	return u.probeProxies(proxies, timeout, false)
}

func (u *URLTestPool) probeProxies(proxies []C.Proxy, timeout time.Duration, publish bool) (map[string]uint16, map[string]struct{}) {
	results := make(map[string]uint16, len(proxies))
	tested := make(map[string]struct{}, len(proxies))
	var mu sync.Mutex

	group := new(errgroup.Group)
	group.SetLimit(urlTestPoolCheckLimit)
	for _, proxy := range proxies {
		proxy := proxy
		group.Go(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			delay, err := proxy.URLTest(ctx, u.testUrl, u.expectedRanges)

			mu.Lock()
			defer mu.Unlock()
			tested[proxy.Name()] = struct{}{}
			if err != nil {
				if publish {
					// the failed member leaves the pool now instead of
					// holding a slot until the check ends
					u.publishPartial(results, tested)
				}
				return nil
			}

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

// proxyByName returns the member with the given name, if it still exists.
func proxyByName(proxies []C.Proxy, name string) C.Proxy {
	for _, proxy := range proxies {
		if proxy.Name() == name {
			return proxy
		}
	}
	return nil
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
		lazy:            option.Lazy,
		suspicious:      make(map[string]struct{}),
	}, nil
}
