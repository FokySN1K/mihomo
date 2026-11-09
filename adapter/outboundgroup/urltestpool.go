package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/outboundgroup/probequeue"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/iface"
	"github.com/metacubex/mihomo/component/reputation"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
)

const (
	defaultURLTestPoolSize           = 10
	defaultURLTestPoolInterval       = 300 * time.Second
	defaultURLTestPoolFullInterval   = 1800 * time.Second
	defaultURLTestPoolDegradedChecks = 3
	// urlTestPoolRecoveryScanLimit bounds how many candidates the emergency
	// test probes once the last pool has already missed: a test that only
	// replayed dead members would repeat the same verdict forever.
	urlTestPoolRecoveryScanLimit    = 20
	urlTestPoolRecoveryCooldown     = 30 * time.Second
	urlTestPoolRecoveryTestInterval = 5 * time.Second
	urlTestPoolRecoveryTestTimeout  = 3 * time.Second
	urlTestPoolRetryReserve         = 500 * time.Millisecond
	// defaultURLTestPoolAttemptTimeout bounds one member dial when the profile
	// does not set `attempt-timeout`: a member that hangs must not spend the
	// whole dial budget of the connection, so the next member of the pool is
	// tried while the caller still waits.
	defaultURLTestPoolAttemptTimeout = 3 * time.Second
	// urlTestPoolCutGrace is how long a member our own attempt-timeout cut stays
	// out of the next choices. A cut is not proof of death — a slow handshake
	// still means the node works — so it must not sit out the whole interval a
	// failed health check earns.
	urlTestPoolCutGrace = 3 * time.Second
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
	// urlTestPoolMaxQuickBackoff caps how far the quick checks stretch while
	// the pool keeps measuring nothing alive: an outage must not cost a sweep
	// of the pool every interval when every member is already known dead.
	urlTestPoolMaxQuickBackoff = 30 * time.Minute
	// urlTestPoolMaxFullBackoff caps the pause between two full sweeps of a
	// pool that has nothing alive. A full sweep probes every candidate of the
	// providers, so a device without a route must not repeat it every full
	// interval.
	urlTestPoolMaxFullBackoff = 6 * time.Hour
	// urlTestPoolCanaryProbes is how many members the canary probe measures
	// before a postponed full sweep is released.
	urlTestPoolCanaryProbes = 3
	// urlTestPoolCanaryTimeout bounds one canary probe: the canary exists to
	// tell a comeback from an outage cheaply, so a slow answer is no answer.
	urlTestPoolCanaryTimeout = 1500 * time.Millisecond
	// urlTestPoolPruneAfterEmptySweeps is how many consecutive empty full
	// sweeps make the sweep skip the nodes below the suspect band of the
	// reputation.
	urlTestPoolPruneAfterEmptySweeps = 2
)

// poolCheckKind selects what a scheduled check measures: the pool members only
// (quick), every candidate of the providers (full), or the few members worth
// one cheap probe while the outage backoff postpones a full sweep (canary).
type poolCheckKind int

const (
	poolQuickCheck poolCheckKind = iota
	poolFullCheck
	poolCanaryCheck
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
	// budget lasts; 0 keeps the single dial per call. An unset value uses
	// defaultURLTestPoolAttemptTimeout, so a hanging member costs one attempt
	// instead of the whole budget.
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
	// reputation remembers how the nodes served real traffic: the store is
	// shared by every group, so a node that failed in one group sinks in the
	// recovery scan of the others as well.
	reputation *reputation.Store

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
	// cutMarks holds the members our own attempt-timeout cut, with the time the
	// grace expires. A cut is not a verdict on the node, so it is kept apart
	// from suspicious: it only stays out of the next choices, and a failure mark
	// that is already there is never shortened by it.
	cutMarks map[string]time.Time
	fastNode C.Proxy
	// measuring is the emergency test of lastPool that runs while the pool has
	// nothing measured alive. Every dial of the exhausted pool joins it instead
	// of probing on its own; it is cleared as the test ends.
	measuring     *poolMeasurement
	lastEmptyFull time.Time
	degradedCount int
	// recoveryMisses counts consecutive emergency tests that found nothing
	// alive. A pool whose own members keep failing is stale after a network
	// change, so the next test moves on to the best candidates of the whole
	// list instead of replaying the same dead members.
	recoveryMisses int
	// emptyStreak counts consecutive full sweeps that measured nothing alive.
	// It stretches the quick checks and the pause between full sweeps, and
	// after urlTestPoolPruneAfterEmptySweeps it narrows the sweep to the nodes
	// that have not sunk below the suspect band of the reputation.
	emptyStreak int
	// nextFullAt is when the next full sweep may run while the pool is in an
	// outage. A zero value means no backoff: only the schedule decides.
	nextFullAt time.Time
	// networkGen is the network generation the pool last saw. A new generation
	// drops the backoff, so the first check on the new network runs at once.
	networkGen uint64
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
	// backoffJitter spreads the retried sweeps of the pools that woke up
	// together; a test substitutes a deterministic one.
	backoffJitter func(time.Duration) time.Duration

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

		u.noteDialFailure(ctx, proxy, err, cut)

		if candidate.exhausted {
			if fallback := u.failedDialFallback(ctx, tried, candidate); fallback != nil {
				proxy = fallback
				tried[proxy.Name()] = struct{}{}
				c, err = proxy.DialContext(ctx, metadata)
				if err != nil {
					u.noteDialFailure(ctx, proxy, err, false)
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

// noteDialFailure records a member that did not carry the dial. The caller's
// budget says nothing about the member: a client that went away is not a
// verdict at all, and a budget that ended before the handshake is a cut. Only
// a failure the member owns keeps it out for the whole interval.
func (u *URLTestPool) noteDialFailure(ctx context.Context, proxy C.Proxy, err error, cut bool) {
	switch {
	case ctx.Err() != nil:
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			log.Debugln("ProxyGroup: %s: %s did not connect within the caller budget", u.Name(), proxy.Name())
			u.onProxyCut(proxy, err)
		}
	case cut:
		log.Debugln("ProxyGroup: %s: %s did not connect within %s", u.Name(), proxy.Name(), u.attemptTimeout)
		u.onProxyCut(proxy, err)
	default:
		u.onProxyFailed(proxy, err)
	}
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

		pc, cut, err := u.packetAttempt(ctx, proxy, metadata)
		if err == nil {
			return pc, nil
		}
		lastErr = err
		u.noteDialFailure(ctx, proxy, err, cut)

		if candidate.exhausted {
			if fallback := u.failedDialFallback(ctx, tried, candidate); fallback != nil {
				proxy = fallback
				tried[proxy.Name()] = struct{}{}
				pc, cut, err = u.packetAttempt(ctx, proxy, metadata)
				if err == nil {
					return pc, nil
				}
				lastErr = err
				u.noteDialFailure(ctx, proxy, err, cut)
			}
			break
		}

		if u.attemptTimeout <= 0 || ctx.Err() != nil {
			break
		}
	}

	return nil, lastErr
}

// packetAttempt opens the packet flow of one member, reporting whether our own
// attempt-timeout cut it while the caller still waited.
func (u *URLTestPool) packetAttempt(ctx context.Context, proxy C.Proxy, metadata *C.Metadata) (C.PacketConn, bool, error) {
	dialCtx, cancel := u.attemptContext(ctx)
	pc, err := proxy.ListenPacketContext(dialCtx, metadata)
	cut := ctx.Err() == nil && dialCtx.Err() != nil
	cancel()
	if err != nil {
		return nil, cut, err
	}

	pc.AppendToChains(u)
	u.noteDialSucceeded(proxy)
	return pc, false, nil
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
		// The emergency test found nothing, but the sweep of every candidate
		// may still return a member. A dial that has no measured route waits
		// for the first alive result of that sweep instead of spending the rest
		// of its budget on a dial the pool already knows is dead.
		if measured := u.awaitCheck(ctx); measured != nil {
			if _, ok := tried[measured.Name()]; !ok && (!udp || measured.SupportUDP()) {
				log.Debugln("ProxyGroup: %s: leaving on %s measured by the full check", u.Name(), measured.Name())
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

	done, _ := u.beginCheck(poolFullCheck)
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

	// A member that failed leaves the pool now instead of holding a slot: the
	// parent asked precisely whether the pool can still carry traffic. The
	// caller's budget bounds only the answer: a member still being probed
	// reports into the pool when it finishes, and a probe that ran out of its
	// own timeout is a dead member like any other.
	results, _ := u.probeProxies(ctx, members, time.Duration(u.testTimeout)*time.Millisecond, true, false)
	if len(results) == 0 {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errNoAliveMember
	}

	u.mu.Lock()
	for name := range results {
		delete(u.suspicious, name)
		delete(u.cutMarks, name)
	}
	u.mu.Unlock()
	u.lastQuick.Store(time.Now())
	return results, nil
}

// parentCheck answers a parent that asked whether the pool can carry traffic at
// all. While the outage backoff holds, a few canary probes stand in for the
// sweep of every candidate: the parent's budget is short, and a failure signal
// alone is not worth sweeping the whole provider again before the schedule says
// so. A sweep the caller cut is not a verdict about the pool, so it is neither
// published nor recorded as a full check: taking the cut for a measurement
// would rebuild the pool from the members one short budget reached and push the
// next sweep further out.
func (u *URLTestPool) parentCheck(ctx context.Context) (map[string]uint16, error) {
	if !u.fullCheckAllowed(time.Now()) {
		return u.canaryProbe(ctx, true)
	}

	results, err := u.GroupBase.URLTest(ctx, u.testUrl, u.expectedRanges)
	if ctx.Err() != nil {
		// the caller gave up: the probes it left behind keep running, but
		// what the sweep reached so far is not the pool's measurement
		if len(results) == 0 {
			return nil, ctx.Err()
		}
		return results, nil
	}
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
	return u.UsableMembers() > 0, true
}

// UsableMembers counts the members that can carry traffic right now: measured
// alive and not sitting out the grace a cut or a failed dial earned. A parent
// reads it to tell a pool that still has something to switch to from one that
// would only repeat the dial that just failed.
func (u *URLTestPool) UsableMembers() int {
	u.mu.RLock()
	names := make(map[string]struct{}, len(u.pool))
	for _, name := range u.pool {
		names[name] = struct{}{}
	}
	now := time.Now()
	// a member cut by our own attempt-timeout cannot carry traffic within the
	// grace either, so a parent must not count it as a reason to stay
	marks := make(map[string]struct{}, len(u.suspicious)+len(u.cutMarks))
	for name, until := range u.suspicious {
		if now.Before(until) {
			marks[name] = struct{}{}
		}
	}
	for name, until := range u.cutMarks {
		if now.Before(until) {
			marks[name] = struct{}{}
		}
	}
	u.mu.RUnlock()

	if len(names) == 0 {
		return 0
	}

	usable := 0
	for _, proxy := range u.GetProxies(false) {
		if _, ok := names[proxy.Name()]; !ok {
			continue
		}
		if _, marked := marks[proxy.Name()]; marked {
			continue
		}
		if proxy.AliveForTestUrl(u.testUrl) {
			usable++
		}
	}
	return usable
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
	recovering := u.measuring != nil || (u.testing.Load() && len(pool) == 0)
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
	// with lazy enabled the checks are driven only by real traffic: reading the
	// group state (Now/MarshalJSON) must never start a test, not even the very
	// first one after a start, so a launch without traffic stays silent.
	if touch || !u.lazy {
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

	// the emergency test belongs to traffic as well: a read of an empty lazy
	// pool reports the state and leaves the checks to the dial
	if touch || !u.lazy {
		u.requestFullCheckOnExhaustion()
	}
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
		return u.bestGuessLocked(proxies), exhausted
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
			continue
		}
		// Nothing measured yet: every candidate reports the max delay, so the
		// reputation decides which one a cold dial leaves on. Once a check has
		// run, the max delay of an alive member means something else and the
		// measured order must not be overridden.
		if delay == bestDelay && delay == 0xffff && !u.hasFullCheck.Load() &&
			u.reputation.Score(proxy.Name()) > u.reputation.Score(best.Name()) {
			best = proxy
		}
	}
	return best
}

// bestGuessLocked returns the member to dial when nothing is measured alive:
// the one with the best reputation, so a cold start of a lazy group does not
// blindly dial the first proxy of the list.
func (u *URLTestPool) bestGuessLocked(proxies []C.Proxy) C.Proxy {
	best := proxies[0]
	bestScore := u.reputation.Score(best.Name())
	for _, proxy := range proxies[1:] {
		if score := u.reputation.Score(proxy.Name()); score > bestScore {
			best = proxy
			bestScore = score
		}
	}
	return best
}

func (u *URLTestPool) isSuspiciousLocked(name string) bool {
	now := time.Now()
	if until, ok := u.suspicious[name]; ok && now.Before(until) {
		return true
	}
	until, ok := u.cutMarks[name]
	return ok && now.Before(until)
}

func (u *URLTestPool) onProxyFailed(proxy C.Proxy, err error) {
	if shouldTrackDialFailure(proxy.Type(), err) {
		u.markSuspicious(proxy.Name())
		if tracksNodeReputation(proxy.Type()) {
			u.reputation.DialFailed(proxy.Name())
		}
	}
	u.onDialFailed(proxy.Type(), err, u.healthCheck)
}

// onProxyCut records a member our own attempt-timeout cut. The node did not
// prove itself dead — a slow handshake still means it works — so it leaves the
// next choices only for the grace, and a member that answers again is back at
// once instead of sitting out the whole interval.
func (u *URLTestPool) onProxyCut(proxy C.Proxy, err error) {
	if shouldTrackDialFailure(proxy.Type(), err) {
		u.markCut(proxy.Name())
	}
	u.onDialFailed(proxy.Type(), err, u.healthCheck)
}

// healthCheck is called by GroupBase when the same proxy failed several times in a row
func (u *URLTestPool) healthCheck() {
	if last, ok := u.lastFull.LoadOk(); ok && time.Since(last) < u.interval {
		return
	}
	u.scheduleCheck(u.allowedCheckKind(poolFullCheck))
}

func (u *URLTestPool) markSuspicious(name string) {
	u.markSuspiciousFor(name, u.suspiciousTTL)
}

// markSuspiciousFor keeps a member that failed a dial out of the next choices
// for the full interval.
func (u *URLTestPool) markSuspiciousFor(name string, ttl time.Duration) {
	u.markFor(&u.suspicious, name, ttl)
}

// markCut keeps a member our own attempt-timeout cut out of the next choices
// for the grace. A cut says nothing about the node, so it never touches the
// failure marks a host reports as suspicious.
func (u *URLTestPool) markCut(name string) {
	u.markFor(&u.cutMarks, name, urlTestPoolCutGrace)
}

// markFor keeps a member out of the next choices until the given ttl ends. A
// later expiry always wins: a cut must not shorten the interval a failed dial
// earned, and neither may a repeated mark shorten itself.
func (u *URLTestPool) markFor(marks *map[string]time.Time, name string, ttl time.Duration) {
	u.mu.Lock()
	if *marks == nil {
		*marks = make(map[string]time.Time)
	}
	next := time.Now().Add(ttl)
	if until, ok := (*marks)[name]; !ok || until.Before(next) {
		(*marks)[name] = next
	}
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
	u.dropBackoffOnNetworkChange()
	u.dropBackoffOnProviderUpdate()

	if !u.hasFullCheck.Load() {
		// a cold pool has nothing measured yet: its first full check is the
		// very thing the backoff cannot postpone
		u.scheduleCheck(poolFullCheck)
		return
	}

	now := time.Now()
	if last, ok := u.lastFull.LoadOk(); !ok || now.Sub(last) >= u.fullInterval {
		if u.fullCheckAllowed(now) {
			u.scheduleCheck(poolFullCheck)
			return
		}
	}
	if last, ok := u.lastQuick.LoadOk(); !ok || now.Sub(last) >= u.quickInterval() {
		u.scheduleCheck(poolQuickCheck)
	}
}

// fullCheckAllowed reports whether the outage backoff lets a full sweep run
// now. Outside the backoff only the schedule decides.
func (u *URLTestPool) fullCheckAllowed(now time.Time) bool {
	u.mu.RLock()
	next := u.nextFullAt
	u.mu.RUnlock()
	return next.IsZero() || !now.Before(next)
}

// allowedCheckKind downgrades a full sweep to the canary probe while the
// outage backoff holds: a failure signal alone is not worth probing every
// candidate of the providers again before the schedule says so.
func (u *URLTestPool) allowedCheckKind(kind poolCheckKind) poolCheckKind {
	if kind == poolFullCheck && !u.fullCheckAllowed(time.Now()) {
		return poolCanaryCheck
	}
	return kind
}

// quickInterval is the delay between two quick checks. While the pool keeps
// measuring nothing alive every empty sweep doubles it, up to
// urlTestPoolMaxQuickBackoff: re-measuring a dead pool only burns battery, and
// the first alive result (or any traffic) ends the streak anyway.
func (u *URLTestPool) quickInterval() time.Duration {
	streak := u.emptySweepStreak()
	if streak <= 0 {
		return u.interval
	}

	wait := u.interval
	for i := 0; i < streak && wait < urlTestPoolMaxQuickBackoff; i++ {
		wait *= 2
	}
	if wait > urlTestPoolMaxQuickBackoff {
		wait = urlTestPoolMaxQuickBackoff
	}
	if wait < u.interval {
		return u.interval
	}
	return wait
}

func (u *URLTestPool) emptySweepStreak() int {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.emptyStreak
}

// dropBackoffOnNetworkChange ends the outage backoff when the device moved to
// another network: a member measured dead on the old network is not a verdict
// on the new one.
func (u *URLTestPool) dropBackoffOnNetworkChange() {
	generation := iface.NetworkGeneration()
	u.mu.RLock()
	seen := u.networkGen
	u.mu.RUnlock()
	if generation == seen {
		return
	}

	u.mu.Lock()
	u.networkGen = generation
	u.mu.Unlock()
	u.exitBackoff("the network changed")
}

// dropBackoffOnProviderUpdate ends the outage backoff when a provider
// published a new proxy list: a subscription that rotated its servers must not
// wait out a backoff the old nodes earned.
func (u *URLTestPool) dropBackoffOnProviderUpdate() {
	if u.emptySweepStreak() == 0 {
		return
	}
	if !u.ProxiesChanged() {
		return
	}
	u.exitBackoff("a provider published a new proxy list")
}

// requestFullCheckOnExhaustion runs a full check when there is no usable proxy left.
// It is throttled by `interval`: after a full check that found nothing alive,
// and after a recent full check that found the pool alive but every dial still
// failed. The latter prevents a failed health check from probing the whole
// provider on every connection.
func (u *URLTestPool) requestFullCheckOnExhaustion() {
	u.dropBackoffOnNetworkChange()
	if !u.shouldRequestFullCheckOnExhaustion(time.Now()) {
		return
	}
	u.scheduleCheck(u.allowedCheckKind(poolFullCheck))
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

func (u *URLTestPool) scheduleCheck(kind poolCheckKind) {
	u.beginCheck(kind)
}

// beginCheck marks the pool as checking and returns the channel that is closed
// when the check ends. started reports whether this call runs it; a check that
// already holds the pool hands its own channel over, so an explicit test joins
// it instead of probing the same candidates again.
func (u *URLTestPool) beginCheck(kind poolCheckKind) (done <-chan struct{}, started bool) {
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

	if kind == poolFullCheck {
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
		switch kind {
		case poolFullCheck:
			u.fullCheck()
		case poolCanaryCheck:
			u.canaryCheck()
		default:
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
		u.checkWithCanary("the pool holds no member")
		return
	}

	proxies := u.GetProxies(true)
	poolProxies, complete := proxiesByNames(proxies, names)
	if !complete {
		// the proxy list has been changed, rebuild the pool
		u.checkWithCanary("the proxy list changed")
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
		log.Debugln("ProxyGroup: %s all pool proxies are unavailable, probe the canary before the next full check", u.Name())
		u.checkWithCanary("the quick check measured nothing")
		return
	}

	u.mu.Lock()
	for name := range results {
		delete(u.suspicious, name)
		delete(u.cutMarks, name)
	}
	u.mu.Unlock()
	u.exitBackoff("a quick check measured a member")

	if u.shouldRunFullCheck(len(results), u.poolTarget(len(proxies))) {
		log.Debugln("ProxyGroup: %s pool is underfilled for %d quick checks, start full health check", u.Name(), u.degradedChecks)
		u.checkWithCanary("the pool is underfilled")
	}
}

// checkWithCanary is what every escalation toward a full sweep goes through:
// the sweep runs only while the schedule allows it, and during the outage
// backoff a cheap canary probe stands in for it. That keeps a lazy group from
// sweeping the whole provider on every wake-up of a pool that has nothing
// alive, which is what drained the battery of an offline device.
func (u *URLTestPool) checkWithCanary(reason string) {
	if u.fullCheckAllowed(time.Now()) {
		u.fullCheck()
		return
	}
	log.Debugln("ProxyGroup: %s: %s, but the outage backoff postpones the full health check: probing %d canary members", u.Name(), reason, urlTestPoolCanaryProbes)
	u.canaryCheck()
}

// canaryCheck probes the few most promising members instead of every
// candidate. It is the retry of an outage: one short probe per canary costs
// almost nothing, and an answer means the network is back, which releases the
// full sweep the backoff postponed. A canary that finds nothing leaves the
// schedule alone, so its own retries cannot push the next sweep further out.
func (u *URLTestPool) canaryCheck() {
	results, _ := u.canaryProbe(context.Background(), false)
	if len(results) == 0 {
		return
	}
	u.fullCheck()
}

// canaryProbe measures the few members worth one cheap probe and publishes what
// answered. It is the cheap stand-in for a full sweep while the outage backoff
// holds. With release set, an answer ends the backoff and lets the postponed
// sweep run in the background, because the caller that asked is a parent with a
// budget of its own and cannot wait for it.
func (u *URLTestPool) canaryProbe(ctx context.Context, release bool) (map[string]uint16, error) {
	candidates := u.canaryCandidates()
	if len(candidates) == 0 {
		return nil, errNoAliveMember
	}

	timeout := time.Duration(u.testTimeout) * time.Millisecond
	if timeout > urlTestPoolCanaryTimeout {
		timeout = urlTestPoolCanaryTimeout
	}
	log.Debugln("ProxyGroup: %s probing %d canary members with a %s timeout", u.Name(), len(candidates), timeout)
	// the canary runs in the foreground lane: a member that answers is what
	// the very next connection leaves on
	results, _ := u.probeProxies(ctx, candidates, timeout, true, true)
	u.storeResults(results)
	u.lastQuick.Store(time.Now())

	if len(results) == 0 {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errNoAliveMember
	}

	u.exitBackoff("a canary member answered")
	if release {
		u.scheduleCheck(poolFullCheck)
	}
	return results, nil
}

// canaryCandidates picks the members worth one cheap probe: the ones the next
// full sweep would probe first, since they are the likeliest to answer.
func (u *URLTestPool) canaryCandidates() []C.Proxy {
	proxies := u.GetProxies(true)
	if !u.proxiesReady(proxies) {
		return nil
	}
	scan := u.orderForCheck(proxies)
	if len(scan) > urlTestPoolCanaryProbes {
		scan = scan[:urlTestPoolCanaryProbes]
	}
	return scan
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
	if tracksNodeReputation(proxy.Type()) {
		u.reputation.DialSucceeded(proxy.Name())
	}
	// the member carried traffic, so the outage is over: the next full sweep
	// follows the usual schedule again instead of the empty-streak backoff
	u.exitBackoff("a dial succeeded")

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
	u.scheduleCheck(poolFullCheck)
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

	proxies := u.GetProxies(true)
	candidates := u.recoveryCandidates(proxies)
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
	log.Debugln("ProxyGroup: %s: no member measured alive, testing %d candidates", u.Name(), len(candidates))

	// The test runs on its own context: a dial that leaves on its first result
	// must not cancel the rest, the members still being probed are what the
	// next connection needs.
	go func() {
		// The probe timeout is the recovery one, never what the starting dial
		// has left: every later dial joins this test, and a short budget of one
		// caller must not turn a slow member into a dead one.
		results, tested := u.probeProxies(context.Background(), candidates, urlTestPoolRecoveryTestTimeout, true, true)
		u.applyRecovery(results, tested, len(candidates))
		u.finishMeasurement(measure)
	}()
	return true
}

// recoveryCandidates picks the members of the emergency test: the last pool
// while it still answers, and the likeliest candidates of the whole list once
// it has missed. A network that changed leaves every member of the old pool
// dead, so replaying that pool forever would only repeat the same verdict.
func (u *URLTestPool) recoveryCandidates(proxies []C.Proxy) []C.Proxy {
	u.mu.RLock()
	names := append([]string(nil), u.lastPool...)
	misses := u.recoveryMisses
	u.mu.RUnlock()

	if misses == 0 {
		if candidates, _ := proxiesByNames(proxies, names); len(candidates) > 0 {
			return candidates
		}
	}

	scan := u.orderForCheck(proxies)
	if len(scan) > urlTestPoolRecoveryScanLimit {
		scan = scan[:urlTestPoolRecoveryScanLimit]
	}
	return scan
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

// awaitCheck waits for the running full check to publish its first alive
// member, bounded by the caller's budget and one first-result wait, and returns
// what the pool has measured alive by then. A dial with nothing measured alive
// spends its budget on a dial the pool already knows is dead otherwise, so the
// wait costs nothing the connection would not have paid. The wait is counted
// from the start of the check, so a sweep that has already been running for
// longer than that is not waited for again by every connection.
func (u *URLTestPool) awaitCheck(ctx context.Context) C.Proxy {
	if best := u.bestMeasuredAlive(); best != nil {
		return best
	}

	u.mu.RLock()
	channel := u.warmCh
	started := u.checkStart
	u.mu.RUnlock()
	if channel == nil {
		// no full check is running, so there is no result to wait for
		return nil
	}

	wait := u.firstResultWait - time.Since(started)
	if left, bounded := recoveryWaitLeft(ctx); bounded && left < wait {
		wait = left
	}
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-channel:
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
	u.mu.Lock()
	if len(results) > 0 {
		u.recoveryMisses = 0
	} else {
		u.recoveryMisses++
	}
	u.mu.Unlock()
	if len(results) > 0 {
		// the emergency test found a member: the outage is over, and the
		// postponed full sweep is due again
		u.exitBackoff("an emergency test measured a member")
	}
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
	results, _ := u.testProxiesStreaming(context.Background(), u.orderForCheck(proxies))
	u.storeResults(results)
	u.applyResults(results)
}

// orderForCheck puts the members that answered last time first, ordered by
// their measured delay, so the first wave of a sweep is the one likeliest to
// find an alive member. A pool that lost its route is then refilled from the
// first results instead of after every candidate of a large provider was
// probed, which is what made an outage look like a long search for a node.
func (u *URLTestPool) orderForCheck(proxies []C.Proxy) []C.Proxy {
	ordered := append([]C.Proxy(nil), proxies...)
	if u.shouldPruneSuspects() {
		// after several empty sweeps the suspect band of the reputation is no
		// longer a hint: those nodes failed every sweep, so skipping them cuts
		// the sweep of a large provider down to the nodes worth a probe. Every
		// other sweep still measures all of them, so the pruning cannot starve
		// a node that is waiting for its network to come back.
		kept := make([]C.Proxy, 0, len(ordered))
		for _, proxy := range ordered {
			if u.reputation.Score(proxy.Name()) >= reputation.Suspect {
				kept = append(kept, proxy)
			}
		}
		if len(kept) > 0 {
			ordered = kept
		}
	}
	// The scores are read once instead of inside the comparator: a sweep of a
	// large provider must not take the store lock for every comparison.
	scores := make(map[string]int, len(ordered))
	for _, proxy := range ordered {
		scores[proxy.Name()] = u.reputation.Score(proxy.Name())
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		left := ordered[i].LastDelayForTestUrl(u.testUrl)
		right := ordered[j].LastDelayForTestUrl(u.testUrl)
		if left != right {
			return left < right
		}
		// Every member that is not measured alive right now shares the max
		// delay, so the reputation decides which of them is worth probing
		// first: a node that carried traffic before is likelier to come back
		// than one that has been failing for days.
		if scores[ordered[i].Name()] != scores[ordered[j].Name()] {
			return scores[ordered[i].Name()] > scores[ordered[j].Name()]
		}
		return ordered[i].Name() < ordered[j].Name()
	})
	return ordered
}

// shouldPruneSuspects reports whether a full sweep should skip the nodes below
// the suspect band. It prunes on every other empty sweep, so their probes come
// back periodically instead of staying skipped for the whole outage.
func (u *URLTestPool) shouldPruneSuspects() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.emptyStreak >= urlTestPoolPruneAfterEmptySweeps && u.emptyStreak%2 == 0
}

func (u *URLTestPool) applyResults(results map[string]uint16) {
	u.publishPool(results, nil, true)

	now := time.Now()
	u.lastFull.Store(now)
	u.lastQuick.Store(now)
	u.hasFullCheck.Store(true)
	if len(results) == 0 {
		u.enterEmptyBackoff(now)
	} else {
		u.exitBackoff("a full check measured a member")
	}
	u.markWarm()
}

// enterEmptyBackoff postpones the next full sweep after one that measured
// nothing alive. Every consecutive empty sweep doubles the wait, up to
// urlTestPoolMaxFullBackoff, and a small jitter keeps the pools of one process
// from waking up together. A sweep that finds a member, a successful dial, a
// provider update or a network change ends the backoff.
func (u *URLTestPool) enterEmptyBackoff(now time.Time) {
	u.mu.Lock()
	u.emptyStreak++
	wait := u.fullBackoffLocked()
	u.nextFullAt = now.Add(wait)
	streak := u.emptyStreak
	u.mu.Unlock()

	if streak == 1 {
		log.Infoln("ProxyGroup: %s measured nothing alive, the next full health check is postponed by %s", u.Name(), wait)
	} else {
		log.Debugln("ProxyGroup: %s measured nothing alive for %d full checks in a row, the next one is postponed by %s", u.Name(), streak, wait)
	}
}

// fullBackoffLocked is the wait of the current empty streak: the full interval
// doubled per empty sweep, capped, with jitter.
func (u *URLTestPool) fullBackoffLocked() time.Duration {
	wait := u.fullInterval
	for streak := 1; streak < u.emptyStreak; streak++ {
		if wait >= urlTestPoolMaxFullBackoff {
			break
		}
		wait *= 2
	}
	if wait > urlTestPoolMaxFullBackoff {
		wait = urlTestPoolMaxFullBackoff
	}
	if u.backoffJitter != nil {
		wait += u.backoffJitter(wait)
	}
	return wait
}

// exitBackoff drops the outage backoff. It is safe to call on a pool that is
// not in one, and it is cheap enough for the dial path.
func (u *URLTestPool) exitBackoff(reason string) {
	u.mu.Lock()
	streak := u.emptyStreak
	if streak == 0 && u.nextFullAt.IsZero() {
		u.mu.Unlock()
		return
	}
	u.emptyStreak = 0
	u.nextFullAt = time.Time{}
	u.lastEmptyFull = time.Time{}
	u.recoveryMisses = 0
	u.mu.Unlock()

	log.Infoln("ProxyGroup: %s left the outage backoff (empty full checks: %d): %s", u.Name(), streak, reason)
}

// publishPartial rebuilds the pool from the results collected so far, so a dial
// that arrives during a full check already routes by a measured delay instead
// of the first proxy of the list. It runs for every finished probe, so the pool
// reaches its full size while the slow members are still being probed and a
// member that failed leaves it at once. The check bookkeeping is left to
// applyResults.
//
// Only an alive result releases the first-result wait: a probe that failed is
// not a member a dial can leave on, and the wait must cover the wave that may
// still find one.
func (u *URLTestPool) publishPartial(results map[string]uint16, tested map[string]struct{}) {
	u.publishPool(results, tested, false)
	if len(results) > 0 {
		u.markWarm()
	}
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
		delete(u.cutMarks, name)
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

// testProxiesStreaming sweeps proxies with the member timeout and publishes
// every measured member as soon as its probe returns.
func (u *URLTestPool) testProxiesStreaming(ctx context.Context, proxies []C.Proxy) (map[string]uint16, map[string]struct{}) {
	return u.probeProxies(ctx, proxies, time.Duration(u.testTimeout)*time.Millisecond, true, false)
}

// probeProxies tests proxies through the shared probe queue and returns the
// delay (ms) of the alive proxies together with every proxy that was actually
// measured, alive or not, so the pool can tell a failed member from one the
// check did not reach. A streaming check publishes every measured member as
// soon as its probe returns, so the pool is usable before the whole list has
// been probed and a cold start routes by the first real result.
//
// A check has no budget of its own: the queue runs every probe to the member
// timeout, and the caller's context bounds only how long the caller waits for
// the answer. A check a parent left behind keeps probing and keeps publishing,
// because the members it measures are what the next connections need.
func (u *URLTestPool) probeProxies(ctx context.Context, proxies []C.Proxy, timeout time.Duration, publish bool, priority bool) (map[string]uint16, map[string]struct{}) {
	// Every pool check, from the scheduled quick sweep to the recovery
	// measurement, funnels its probes through here, so this is the one place
	// that knows the whole set before the first probe answers.
	endCheck := beginCheckScope(u.Name(), u.testUrl, proxies)
	defer endCheck()

	results := make(map[string]uint16, len(proxies))
	tested := make(map[string]struct{}, len(proxies))
	var mu sync.Mutex

	probes := make([]probequeue.Probe, 0, len(proxies))
	for _, proxy := range proxies {
		probes = append(probes, probequeue.Probe{
			Proxy:    proxy,
			URL:      u.testUrl,
			Expected: u.expectedRanges,
			Timeout:  timeout,
			Priority: priority,
		})
	}

	// The queue decides when the probe runs; the pool's only limit is the
	// timeout of the member itself. A group that checks without `lazy` routes
	// the profile's traffic by its own answers, so its check runs in the
	// foreground lane; a lazy group checks only when traffic wakes it and
	// takes the background ring.
	submit := probequeue.Submit
	if !u.lazy {
		submit = probequeue.SubmitForeground
	}
	check := submit(u.Name(), probes, func(result probequeue.Result) {
		mu.Lock()
		defer mu.Unlock()

		tested[result.Name] = struct{}{}
		if result.Err != nil {
			if publish {
				// the failed member leaves the pool now instead of
				// holding a slot until the check ends
				u.publishPartial(results, tested)
			}
			if !errors.Is(result.Err, context.Canceled) {
				u.reputation.ProbeFailed(result.Name)
			}
			return
		}

		results[result.Name] = result.Delay
		if publish {
			// published under the results lock: a later publish always
			// carries every earlier result, so the pool cannot shrink back
			u.publishPartial(results, tested)
		}
	})
	check.Wait(ctx)

	// The answer is a snapshot: a check the caller left behind keeps probing
	// and keeps publishing through the callback above, but it must not write
	// the maps this caller reads.
	mu.Lock()
	defer mu.Unlock()
	return copyDelayResults(results), copyTestedNames(tested)
}

func copyDelayResults(results map[string]uint16) map[string]uint16 {
	copied := make(map[string]uint16, len(results))
	for name, delay := range results {
		copied[name] = delay
	}
	return copied
}

func copyTestedNames(tested map[string]struct{}) map[string]struct{} {
	copied := make(map[string]struct{}, len(tested))
	for name := range tested {
		copied[name] = struct{}{}
	}
	return copied
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

// tracksNodeReputation reports whether a failure or a success of this adapter
// says anything about a node: the built-in outbounds are not nodes of a
// subscription and must not take space in the store.
func tracksNodeReputation(adapterType C.AdapterType) bool {
	switch adapterType {
	case C.Direct, C.Compatible, C.Reject, C.Pass, C.RejectDrop:
		return false
	}
	return true
}

// randomBackoffJitter spreads the retried sweeps by up to a tenth of the wait,
// so the pools of one process do not wake up at the same moment.
func randomBackoffJitter(wait time.Duration) time.Duration {
	if wait <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(wait/10) + 1))
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

	attemptTimeout := defaultURLTestPoolAttemptTimeout
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
		reputation:      reputation.Default(),
		preWarmRecheck:  urlTestPoolPreWarmRecheck,
		backoffJitter:   randomBackoffJitter,
		networkGen:      iface.NetworkGeneration(),
		// a member that failed a dial is skipped until the next quick check
		// could have re-measured it
		suspiciousTTL: interval,
		suspicious:    make(map[string]time.Time),
		cutMarks:      make(map[string]time.Time),
	}, nil
}
