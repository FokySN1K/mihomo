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
	// tried while the caller still waits. A working dial is one round trip;
	// the second and a half covers a slow handshake of a node that still works
	// and leaves the rest of the tunnel budget to the next member.
	defaultURLTestPoolAttemptTimeout = 1500 * time.Millisecond
	// urlTestPoolCutGrace is how long a member our own attempt-timeout cut stays
	// out of the next choices. A cut is not proof of death — a slow handshake
	// still means the node works — so it must not sit out the whole interval a
	// failed health check earns.
	urlTestPoolCutGrace = 3 * time.Second
	// urlTestPoolStormCuts is how many different members our own
	// attempt-timeout has to cut within urlTestPoolStormWindow before the pool
	// reads it as a storm and checks itself early. One cut may be one slow
	// node; several members cut within seconds say the route itself is slow,
	// and a check is what tells the pool which of them still answer.
	urlTestPoolStormCuts   = 3
	urlTestPoolStormWindow = 3 * time.Second
	// urlTestPoolStormThrottle bounds how often a storm may start a check: a
	// route that stays slow keeps cutting, and a check per dial would turn the
	// outage into a sweep of the pool behind every connection.
	urlTestPoolStormThrottle = 20 * time.Second
	// urlTestPoolRecoveryWait is the head start an exhausted dial gives the
	// emergency test before it falls back to the last known good proxy. The
	// first probe of the test usually answers within it, so the dial leaves on
	// a measured member instead of paying for a blind dial of the old one.
	urlTestPoolRecoveryWait = 500 * time.Millisecond
	// defaultURLTestPoolFirstResultWait caps how long a dial of a cold start
	// waits for the first measured member. The wait is also bounded by the probe
	// timeout (a result cannot arrive sooner) and is counted from the start of
	// the check, so a check cycle never delays dials longer than this. It is
	// short on purpose: a blind dial of a decent node connects sooner than the
	// first probe answers, and the pool corrects a failed guess right after.
	defaultURLTestPoolFirstResultWait = 300 * time.Millisecond
	// defaultURLTestPoolFirstWaveWait caps how long the first dial of a cold
	// pool waits for the sweep to fill the pool. The dial leaves earlier as
	// soon as the sweep answered for `pool-size` members with at least one
	// alive, and the window is counted from the start of the check, so the
	// whole check cycle delays dials by at most this.
	defaultURLTestPoolFirstWaveWait = 1500 * time.Millisecond
	// defaultURLTestPoolLazyWaveWait is the window of a lazy pool: its warm
	// check measures only the handful of members the reputation trusts, so a
	// full sample is never expected of it and waiting for one would only delay
	// the dial.
	defaultURLTestPoolLazyWaveWait = 300 * time.Millisecond
	// urlTestPoolReputationBand is how many reputation points separate two
	// bands of the pre-test order. Inside one band the measured delay decides
	// which member is probed first; across bands the reputation does, so a
	// proven node is measured before an unknown one without letting the noise
	// of a single strike reorder the wave.
	urlTestPoolReputationBand = 10
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
	// urlTestPoolNetworkRetryPause is the pause before the first scheduled
	// check on a new network retries the probes that failed: the route and DNS
	// of a fresh network settle within it, and a handshake that timed out
	// meanwhile is not a verdict on the member.
	urlTestPoolNetworkRetryPause = 500 * time.Millisecond
	// urlTestPoolDataStrike is how long a member that accepted the handshake
	// and then went silent stays out of the next dials. The first strike is
	// cheap on purpose: one flow may have hit a broken destination, so the
	// member gets its place back within a reconnect, and only a member that
	// keeps dying after the handshake backs off further.
	urlTestPoolDataStrike = 30 * time.Second
	// urlTestPoolDataStrikeMax caps that demotion: a node that keeps accepting
	// the handshake and never carrying the flow must stop returning every
	// interval.
	urlTestPoolDataStrikeMax = 5 * time.Minute
	// urlTestPoolDataStrikeWindow resets the strike count: strikes spread wider
	// than this are separate incidents and do not escalate.
	urlTestPoolDataStrikeWindow = 15 * time.Minute
	// urlTestPoolStrikeDedupe is how close two silent flows have to be to count
	// as one incident. The flows of one connection die together when a member
	// goes silent, and each of them would otherwise double the demotion of a
	// member that took a single hit.
	urlTestPoolStrikeDedupe = 10 * time.Second
	// urlTestPoolFirstByteFloor is the least time the reply watchdog gives a
	// handshake, even to a member that always answers at once: a TLS handshake
	// of a busy server takes a moment even on a healthy route.
	urlTestPoolFirstByteFloor = 800 * time.Millisecond
	// urlTestPoolFirstByteFactor scales the watchdog by the measured answer
	// time of the member, so a slow but working node is not cut on its own
	// normal latency.
	urlTestPoolFirstByteFactor = 3
	// urlTestPoolFirstByteCap bounds the watchdog: past it, waiting is worse
	// than a retry on another member.
	urlTestPoolFirstByteCap = 2500 * time.Millisecond
	// urlTestPoolFirstByteWeight is the EWMA weight of a fresh answer time: a
	// single slow answer moves the baseline by a quarter of its distance.
	urlTestPoolFirstByteWeight = 4
	// urlTestPoolDataEvidenceCap bounds the per-member evidence maps: a
	// subscription that rotates names must not grow them for the whole session.
	urlTestPoolDataEvidenceCap = 512
	// urlTestPoolMarkedProbeTimeout is the probe budget of a member the pool
	// already marked as out of play: a dial it failed or a flow that died
	// after the handshake. Such a member has to prove it is back, and one that
	// answers does so within this budget, so a sweep must not spend the whole
	// member timeout on the nodes the previous one already measured dead.
	urlTestPoolMarkedProbeTimeout = 1500 * time.Millisecond
	// urlTestPoolPromotionFreshness bounds the age of the measurement a member
	// is promoted on. A measured delay carries no age of its own: the proxy
	// keeps the last number until a check overwrites it, so a node that died
	// after the previous sweep still reports the fastest delay of the pool and
	// takes the next dial down with it. A candidate measured longer ago than
	// this, or never, is verified by one background probe before it replaces
	// the member a dial is already using.
	urlTestPoolPromotionFreshness = 2 * time.Minute
	// urlTestPoolFirstWaveCoverage is how many members the transport coverage
	// of a sweep may move into its head. The queue starts about forty probes
	// at once, so the head of the order is the wave; four slots are enough to
	// keep every transport of a subscription in it without pushing the
	// measured leaders out.
	urlTestPoolFirstWaveCoverage = 4
)

// poolCheckKind selects what a scheduled check measures: the pool members only
// (quick), every candidate of the providers (full), the few members worth one
// cheap probe while the outage backoff postpones a full sweep (canary), or the
// reputation leaders a lazy group probes before its first dial (warm).
type poolCheckKind int

const (
	poolQuickCheck poolCheckKind = iota
	poolFullCheck
	poolCanaryCheck
	poolWarmCheck
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
	// FirstWaveWait caps how long the first dial of a cold pool waits for the
	// sweep to fill the pool, in milliseconds: the dial leaves as soon as the
	// sweep answered for `pool-size` members with at least one alive, and never
	// later than this window. 0 disables the wait. An unset value uses 1500ms,
	// or 300ms for a lazy group, whose warm check measures only the members the
	// reputation trusts.
	FirstWaveWait *int `group:"first-wave-wait,omitempty"`
	// Ordered places the pool in the profile-wide probe chain: the scheduled
	// cold checks run wave by wave, the explicit numbers ascending first and
	// the pools without one following in declaration order. Pools with equal
	// numbers share a wave. It changes only the order of the checks, never
	// what they measure.
	Ordered *int `group:"ordered,omitempty"`
}

// URLTestPool keeps a fixed pool of the fastest proxies.
// A quick health check tests only the pool members every `interval`,
// a full health check tests all proxies every `full-interval` and rebuilds
// the pool. When the pool stays underfilled (some members are dead) for
// several quick checks in a row, a full check is started early to refill it.
// The pool also fills itself before the first dial: the executor asks it to
// warm up as soon as the config is applied, so the first connection does not
// pay for the cold-start probe. A lazy group does not sweep its candidates at
// a start: it measures only the members the reputation trusts, which fills the
// pool for the first dial without a sweep of the whole provider. On a cold
// start every measured member is published as soon as its probe returns, and a
// dial waits for a sample of `pool-size` answers with at least one alive member
// (bounded by `first-wave-wait` and by half of the caller's budget), so it
// leaves on the best of a measured sample instead of the first proxy of the
// list only because nothing has been measured.
//
// A pool that sets `ordered` takes part in the profile-wide chain: the queue
// serves the scheduled cold checks one wave at a time, the explicit numbers
// ascending and the pools that set none following in declaration order, so the
// profile decides which group finds its member first instead of the sorted
// group order doing it. A dial that arrives for a pool promotes its check to
// the head of the chain; `probe-order: parallel` restores the plain urgent
// round-robin.
//
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
	firstWaveWait   time.Duration
	attemptTimeout  time.Duration
	recoveryWait    time.Duration
	lazy            bool
	// ordered is the `ordered` option of the pool: its place in the profile-
	// wide probe chain. nil means the profile gave the pool no number.
	ordered *int
	// reputation remembers how the nodes served real traffic: the store is
	// shared by every group, so a node that failed in one group sinks in the
	// recovery scan of the others as well.
	reputation *reputation.Store

	mu       sync.RWMutex
	selected string
	pool     []string
	lastPool []string
	lastGood string
	// view is the pool resolved against one snapshot of the proxy list,
	// published so a dial, a MarshalJSON or a SupportUDP ask does not resolve
	// the pool again. It is replaced, never edited.
	view atomic.Pointer[poolView]
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
	// cutStorm holds the recent cuts by member, so the pool can tell a storm of
	// attempt-timeouts from one slow node, and stormChecked is when that storm
	// last started a check (the throttle of the next one).
	cutStorm     []poolCut
	stormChecked time.Time
	// dataMarks holds the members that accepted the handshake and went silent,
	// with the time the mark expires and the strike count behind it. A check
	// that measures the node alive does not clear it: a probe answers in
	// milliseconds while the flow a user waits for hung, and that is exactly
	// the outage the mark exists to catch. Only a real answered byte clears it.
	dataMarks map[string]*poolDataMark
	// firstBytes is the measured answer time of every member, used to size the
	// reply watchdog of the next connection.
	firstBytes map[string]*poolFirstByte
	fastNode   C.Proxy
	fastReason string
	// eventNow and eventReason are the state the history last recorded, so a
	// read of the group writes an event only when the route actually changed.
	eventNow    string
	eventReason string
	// measuredAt is when the pool last saw each member measured alive, by a
	// check or by a dial that reached it. A proxy reports its last delay
	// without an age, so this map is what keeps a stale measurement from
	// promoting a member the pool cannot trust any more.
	measuredAt map[string]time.Time
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
	// lastCheckGen is the network generation the last scheduled check measured
	// under. A check that starts on another generation retries its failed
	// probes once, because a member that timed out while the network was
	// settling is not dead.
	lastCheckGen uint64
	// checkGen is the generation the check that holds `testing` began under. A
	// sweep asked for while a check already runs compares against it: a check
	// of the old route has to be followed by a fresh one, and one that already
	// measures the new route answers the question itself.
	checkGen uint64
	// warmCh is open while a full check is running and is closed once the first
	// measured member is published; checkStart anchors the first-result wait, so
	// the whole check cycle never delays dials longer than the cap.
	warmCh     chan struct{}
	checkStart time.Time
	// warmFill is the gate a dial of a cold pool waits on: it closes once the
	// sweep answered for `pool-size` members with at least one alive, so the
	// dial leaves on the best of a sample instead of the first answer. A sweep
	// too slow to fill it is left on the window instead, and warmDelivered
	// counts its answers so a dial can tell a sweep that is producing results
	// from one still queued behind another group.
	warmFill      chan struct{}
	warmDelivered int

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
	// pendingFull is set when a full sweep is asked for while a check of
	// another network generation holds the pool; the check re-arms it as it
	// ends, so the request is not lost with the old route.
	pendingFull atomic.Bool
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

// poolDataMark is the evidence a member left after the handshake: it accepted
// the first write and never answered. The strike count outlives the expiry, so
// a member that keeps dying after the handshake backs off further every time;
// a member that carries a real answer resets it.
type poolDataMark struct {
	until   time.Time
	strikes int
	last    time.Time
}

// poolCut is one member our own attempt-timeout cut, stamped so the pool can
// count how many different members were cut within a window.
type poolCut struct {
	name string
	at   time.Time
}

// poolFirstByte is the running average of how long a member takes to answer
// the handshake, in the EWMA form: the watchdog of the next connection is
// scaled by it, so a slow but working member is not cut on its own latency.
type poolFirstByte struct {
	avg  time.Duration
	hits int
	last time.Time
}

// poolView is the pool resolved against one snapshot of the proxy list. It is
// immutable once published: a reader checks the generations it was built under
// and lets the pool build a new one instead of editing this one.
type poolView struct {
	// members is the pool in its own order. A name the list no longer holds is
	// skipped, and an empty result tells the caller the pool is stale.
	members []C.Proxy
	listGen uint64
	poolSig uint64
}

// pickLists returns the candidates the choice picks among and the list the
// fallbacks scan: the pool itself, or its UDP members for a packet flow.
// The capability itself is read live and not cached with the view: a nested
// group in the list can change it without a new provider version, and the scan
// is what a packet dial cost before the view existed.
func (v *poolView) pickLists(udp bool, list []C.Proxy) (candidates, scan []C.Proxy) {
	if !udp {
		return v.members, list
	}
	scan = udpProxies(list)
	if len(scan) == 0 {
		// A list that filters down to nothing is left as it is: the dial then
		// reports the failure instead of silently leaving the group.
		return v.members, list
	}
	return udpProxies(v.members), scan
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

// poolSignature stamps the names the pool holds. Building the view costs a
// scan of the whole proxy list, so it is reused while this stamp and the list
// generation match; hashing the handful of pool names is far cheaper than the
// scan, and no writer of the pool has to remember to bump a counter.
func poolSignature(names []string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	sig := uint64(offset64)
	for _, name := range names {
		sig ^= uint64(len(name))
		sig *= prime64
		for i := 0; i < len(name); i++ {
			sig ^= uint64(name[i])
			sig *= prime64
		}
	}
	return sig
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
		if N.NeedHandshake(c) {
			// The dial only accepted the connection: the first byte the
			// member answers with is what proves it carries traffic, so the
			// reputation is credited there and not twice. The route itself
			// works already, so the outage bookkeeping runs now.
			u.noteDialAccepted(proxy)
			c = u.observeDataPlane(proxy, c, true)
		} else {
			// The dial of this protocol already completed its own handshake
			// (QUIC, an authentication exchange), so it is proof of life. The
			// observer still watches the data plane: a node that stops
			// answering after it is what the watchdog catches.
			u.noteDialSucceeded(proxy)
			c = u.observeDataPlane(proxy, c, false)
		}
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
	if errors.Is(context.Cause(ctx), errCandidateBudget) {
		// A parent cut its whole candidate share of the budget: the member
		// was not measured, and the cut says nothing about it.
		log.Debugln("ProxyGroup: %s: %s was cut with the candidate budget of the parent", u.Name(), proxy.Name())
		return
	}
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
	if fallback := u.blindDialFallback(proxies); fallback != nil {
		if _, ok := tried[fallback.Name()]; !ok {
			log.Debugln("ProxyGroup: %s: %s found by the emergency test failed, dialing the blind fallback %s", u.Name(), candidate.proxy.Name(), fallback.Name())
			return fallback
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
	// working instead of losing them whenever the fast member changes. The
	// answer is read from the cached list on every call, not from the pool
	// view: a nested group can change its capability without a new provider
	// version, and a cached answer would keep the group out of the packet path
	// (or inside it) until the list itself changed.
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
	proxy, _ := u.nowState(false)
	return proxy.Name()
}

// nowState returns the member a read of the group routes through and the
// reason it was chosen, so MarshalJSON publishes both from the same choice.
func (u *URLTestPool) nowState(touch bool) (C.Proxy, string) {
	proxy, _, reason := u.fastForTrafficState(touch, false)
	return proxy, reason
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
	previous := u.selected
	u.selected = name
	u.fastNode = nil
	u.fastReason = ""
	u.mu.Unlock()
	if previous != name {
		recordPinEvent(groupName(u.GroupBase), previous, name)
	}
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
	if !u.hasUsableNode(members) {
		// The caller waits on this answer, and the pool has nothing measured
		// alive to route by: its check takes the head of the ordered chain
		// instead of the wave the profile gave the pool.
		probequeue.Promote(u.Name())
	}
	results, _ := u.probeProxies(ctx, members, time.Duration(u.testTimeout)*time.Millisecond, true, false, false)
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
	proxies, listGen := u.GetProxiesWithGen(false)
	u.mu.Lock()
	members := u.viewLocked(proxies, listGen).members
	now := time.Now()
	usable := 0
	for _, proxy := range members {
		// a member cut by our own attempt-timeout cannot carry traffic within
		// the grace either, so a parent must not count it as a reason to stay;
		// the same holds for one that went silent after the handshake
		if !u.markUntilLocked(proxy.Name(), now).IsZero() {
			continue
		}
		if proxy.AliveForTestUrl(u.testUrl) {
			usable++
		}
	}
	u.mu.Unlock()
	return usable
}

// hasUsableNode reports whether the members a check is about to probe already
// contain one that answered on the test URL and is not marked as out of play.
// A pool that has none is cold: it has nothing measured to route a dial by, so
// its sweep must not queue behind the sweeps of the groups that do.
//
// AliveForTestUrl is not the signal here: a proxy starts alive, so a group that
// never measured anything would look warm. A per-URL delay exists only after a
// measurement, and LastDelayForTestUrl reports measuredURLDelay when there is
// none. The member the profile fixed counts as found without a measurement:
// pinning a name is a decision, not a cold guess.
func (u *URLTestPool) hasUsableNode(proxies []C.Proxy) bool {
	now := time.Now()
	u.mu.RLock()
	selected := u.selected
	marks := make(map[string]struct{}, len(proxies))
	for _, proxy := range proxies {
		if !u.markUntilLocked(proxy.Name(), now).IsZero() {
			marks[proxy.Name()] = struct{}{}
		}
	}
	u.mu.RUnlock()

	for _, proxy := range proxies {
		if _, marked := marks[proxy.Name()]; marked {
			continue
		}
		if selected != "" && proxy.Name() == selected && proxy.AliveForTestUrl(u.testUrl) {
			return true
		}
		if proxy.LastDelayForTestUrl(u.testUrl) != measuredURLDelay {
			return true
		}
	}
	return false
}

// SelfTestTarget implements SelfTester: the URL and status the pool measures
// its members against.
func (u *URLTestPool) SelfTestTarget() (string, utils.IntRanges[uint16]) {
	return u.testUrl, u.expectedRanges
}

// ChainOrder reports the `ordered` number of the pool, nil when the profile
// gave it none. The config parser reads it to build the probe chain.
func (u *URLTestPool) ChainOrder() *int {
	return u.ordered
}

// MarshalJSON implements C.ProxyAdapter
func (u *URLTestPool) MarshalJSON() ([]byte, error) {
	all := make([]string, 0)
	for _, proxy := range u.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	nowProxy, reason := u.nowState(false)
	now := nowProxy.Name()

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
	silent := make([]string, 0, len(u.dataMarks))
	for name, mark := range u.dataMarks {
		if ts.Before(mark.until) {
			silent = append(silent, name)
		}
	}
	u.mu.RUnlock()
	sort.Strings(suspicious)
	sort.Strings(silent)

	fixedOK := fixedActive(selected, now)

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
		"fixedActive":    fixedOK,
		"nowReason":      reason,
		"lastGood":       lastGood,
		"recovering":     recovering,
		"suspicious":     suspicious,
		"dataSilent":     silent,
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
	return u.fastForTraffic(touch, false)
}

// fastForPacket is fastForDial restricted to the members that carry UDP, so a
// packet flow of a mixed pool never lands on a member that drops it.
func (u *URLTestPool) fastForPacket(touch bool) (C.Proxy, bool) {
	return u.fastForTraffic(touch, true)
}

// fastForTraffic keeps the dial-facing signature; the reason is only needed by
// reads of the group state.
func (u *URLTestPool) fastForTraffic(touch bool, udp bool) (C.Proxy, bool) {
	proxy, exhausted, _ := u.fastForTrafficState(touch, udp)
	return proxy, exhausted
}

func (u *URLTestPool) fastForTrafficState(touch bool, udp bool) (C.Proxy, bool, string) {
	proxy, exhausted, reason := u.fastForTrafficChoice(touch, udp)
	if !udp {
		// A UDP-filtered choice is a view of the same group, not a change of
		// the member it routes through, so only the general choice is a
		// history event.
		u.recordNowEvent(proxy, reason)
	}
	return proxy, exhausted, reason
}

// recordNowEvent stores a change of the member the group routes through. The
// dial path reads the group on every connection, so the pair is compared under
// the lock and only a change builds an event.
func (u *URLTestPool) recordNowEvent(now C.Proxy, reason string) {
	name := ""
	if now != nil {
		name = now.Name()
	}

	u.mu.Lock()
	if u.eventNow == name && u.eventReason == reason {
		u.mu.Unlock()
		return
	}
	from := u.eventNow
	u.eventNow = name
	u.eventReason = reason
	u.mu.Unlock()

	RecordGroupEvent(GroupEvent{
		Group:  groupName(u.GroupBase),
		Type:   GroupEventNow,
		From:   from,
		To:     name,
		Reason: reason,
	})
}

func (u *URLTestPool) fastForTrafficChoice(touch bool, udp bool) (C.Proxy, bool, string) {
	// with lazy enabled the checks are driven only by real traffic: reading the
	// group state (Now/MarshalJSON) must never start a test, not even the very
	// first one after a start, so a launch without traffic stays silent.
	if touch || !u.lazy {
		u.maybeScheduleCheck()
	}

	proxies, listGen := u.GetProxiesWithGen(touch)
	if len(proxies) == 0 {
		return u.EmptyFallback(), false, NowReasonEmpty
	}

	// One critical section for the whole choice: the view is taken or rebuilt,
	// the member is picked and the fallback of an exhausted pool is chosen
	// under the same lock, so a dial no longer takes it twice.
	u.mu.Lock()
	candidates, scan := u.viewLocked(proxies, listGen).pickLists(udp, proxies)
	proxy, exhausted, verify := u.pickFastLocked(scan, candidates)
	reason := u.fastReason
	if reason == "" {
		reason = NowReasonEmpty
	}

	var fallback C.Proxy
	fallbackReason := reason
	switchedTo := ""
	if exhausted {
		if picked := u.blindDialFallbackLocked(scan); picked != nil {
			fallback = picked
			if picked.Name() != proxy.Name() {
				// the member the pool already marked as out of play is the one it
				// would have dialed before, so the log names both and a field log
				// shows which memory the dial left on instead
				switchedTo = picked.Name()
				fallbackReason = NowReasonEmpty
				if u.lastGood == picked.Name() {
					fallbackReason = NowReasonLastGood
				}
			}
		}
	}
	u.mu.Unlock()

	if switchedTo != "" {
		log.Debugln("ProxyGroup: %s: %s is marked out of play, dialing %s instead", u.Name(), proxy.Name(), switchedTo)
	}
	if verify != nil && touch {
		// The pool held the promotion back: the candidate is measured by one
		// background probe, and the next dial promotes it once the answer
		// stamps the measurement. The probe runs outside the pool lock. A read
		// of the group state (Now/MarshalJSON) must not start it: reading a
		// group is not traffic, and a lazy group must stay silent without it.
		u.startPromotionProbe(verify)
	}

	if fallback != nil {
		return fallback, true, fallbackReason
	}
	return proxy, exhausted, reason
}

func (u *URLTestPool) currentFastReason() string {
	u.mu.RLock()
	reason := u.fastReason
	u.mu.RUnlock()
	if reason == "" {
		return NowReasonEmpty
	}
	return reason
}

func (u *URLTestPool) lastGoodLocked(proxies []C.Proxy) C.Proxy {
	if u.lastGood == "" {
		return nil
	}

	for _, proxy := range proxies {
		if proxy.Name() == u.lastGood {
			return proxy
		}
	}
	return nil
}

// blindDialFallback picks the member an exhausted dial leaves on when nothing
// is measured alive. The last known good keeps the dial while no mark covers
// it: it is the only member known to have carried traffic. A member the pool
// already marked as out of play is not dialed on that memory alone — a flow
// that hung on it would hang again — so the dial leaves on the best member the
// marks do not cover, and only a pool whose every member is marked falls back
// to the one whose mark ends first.
func (u *URLTestPool) blindDialFallback(proxies []C.Proxy) C.Proxy {
	if len(proxies) == 0 {
		return nil
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	return u.blindDialFallbackLocked(proxies)
}

// blindDialFallbackLocked is blindDialFallback for a caller that already holds
// u.mu: the dial choice takes the lock once and keeps the fallback inside it.
// It must be called with u.mu held.
func (u *URLTestPool) blindDialFallbackLocked(proxies []C.Proxy) C.Proxy {
	if len(proxies) == 0 {
		return nil
	}

	now := time.Now()

	clean := make([]C.Proxy, 0, len(proxies))
	for _, proxy := range proxies {
		if u.markUntilLocked(proxy.Name(), now).IsZero() {
			clean = append(clean, proxy)
		}
	}

	if last := u.lastGoodLocked(clean); last != nil {
		return last
	}
	if best := u.bestAliveLocked(clean, false); best != nil {
		return best
	}
	if len(clean) > 0 {
		return u.bestGuessLocked(clean)
	}

	var soonest C.Proxy
	var soonestUntil time.Time
	for _, proxy := range proxies {
		until := u.markUntilLocked(proxy.Name(), now)
		if soonest == nil || until.Before(soonestUntil) {
			soonest = proxy
			soonestUntil = until
		}
	}
	return soonest
}

// markUntilLocked returns the end of the longest mark the pool holds on the
// member: a dial it failed, our own attempt-timeout cut, or a flow that died
// after the handshake. A zero time means the member is not marked.
func (u *URLTestPool) markUntilLocked(name string, now time.Time) time.Time {
	var until time.Time
	if t, ok := u.suspicious[name]; ok && now.Before(t) {
		until = t
	}
	if t, ok := u.cutMarks[name]; ok && now.Before(t) && t.After(until) {
		until = t
	}
	if mark, ok := u.dataMarks[name]; ok && now.Before(mark.until) && mark.until.After(until) {
		until = mark.until
	}
	return until
}

// failedUntilLocked is markUntilLocked without the grace of our own cut: a cut
// says nothing about the node, so a probe must not be shortened for it.
func (u *URLTestPool) failedUntilLocked(name string, now time.Time) time.Time {
	var until time.Time
	if t, ok := u.suspicious[name]; ok && now.Before(t) {
		until = t
	}
	if mark, ok := u.dataMarks[name]; ok && now.Before(mark.until) && mark.until.After(until) {
		until = mark.until
	}
	return until
}

func (u *URLTestPool) pickFast(proxies []C.Proxy, touch bool) (C.Proxy, bool) {
	u.mu.Lock()
	candidates, scan := u.viewForLocked(proxies).pickLists(false, proxies)
	proxy, exhausted, verify := u.pickFastLocked(scan, candidates)
	u.mu.Unlock()
	if verify != nil && touch {
		// The pool held the promotion back: the candidate is measured by one
		// background probe, and the next dial promotes it once the answer
		// stamps the measurement. The probe runs outside the pool lock. A read
		// of the group state (Now/MarshalJSON) must not start it: reading a
		// group is not traffic, and a lazy group must stay silent without it.
		u.startPromotionProbe(verify)
	}
	return proxy, exhausted
}

// pickFastLocked chooses the member a dial leaves on among candidates, the pool
// resolved against scan. It reports the member a background probe still has to
// verify before the pool promotes it over the member the dial is already using.
// It must be called with u.mu held.
func (u *URLTestPool) pickFastLocked(scan, candidates []C.Proxy) (C.Proxy, bool, C.Proxy) {
	// A pin is the user's decision, not a health hint: while the pinned member
	// is not measured dead the pool dials it even when a mark names a risk.
	// A pin that IS measured dead (or marked out of play) yields to a live pool
	// member instead of blackholing the route: `fixed` keeps the pick and
	// `fixedActive` turns false with the `fixed-suspicious` reason, so a host
	// can say the pin is skipped and why. The route returns to the pin as soon
	// as the member answers again. `fixed-unavailable` is left for the pin whose
	// member left the list: that one cannot be dialed at all.
	fixedReason := ""
	if u.selected != "" {
		var pinned C.Proxy
		for _, proxy := range scan {
			if proxy.Name() == u.selected {
				pinned = proxy
				break
			}
		}
		if pinned != nil {
			if pinned.AliveForTestUrl(u.testUrl) && !u.isSuspiciousLocked(pinned.Name()) {
				u.fastNode = pinned
				u.fastReason = NowReasonFixed
				return pinned, false, nil
			}
			// The pin is out of play: a live member carries the dial when the
			// pool still has one, and only a pool with nothing else left keeps
			// the pin as the least blind choice.
			live := candidates
			if len(live) == 0 {
				live = scan
			}
			if alive := u.bestAliveLocked(live, true); alive != nil {
				u.fastNode = alive
				u.fastReason = NowReasonFixedSuspicious
				return alive, false, nil
			}
			u.fastNode = pinned
			u.fastReason = NowReasonFixed
			return pinned, false, nil
		}
		fixedReason = NowReasonFixedUnavailable
	}

	stale := len(u.pool) > 0 && len(candidates) == 0
	if len(candidates) == 0 {
		candidates = scan
	}

	best := u.bestAliveLocked(candidates, true)
	exhausted := best == nil || stale
	if best == nil {
		// the pool is temporarily unusable, fall back to the rest of the proxies
		// while the full health check is being requested
		best = u.bestAliveLocked(candidates, false)
		if best == nil && len(candidates) != len(scan) {
			best = u.bestAliveLocked(scan, false)
		}
	}
	if best == nil {
		u.fastNode = nil
		if fixedReason != "" {
			u.fastReason = fixedReason
		} else {
			u.fastReason = NowReasonEmpty
		}
		return u.bestGuessLocked(scan), exhausted, nil
	}

	now := time.Now()
	if u.fastNode != nil && u.fastNode.AliveForTestUrl(u.testUrl) &&
		!u.isSuspiciousLocked(u.fastNode.Name()) && containsProxy(candidates, u.fastNode) {
		bestDelay := best.LastDelayForTestUrl(u.testUrl)
		if u.fastNode.LastDelayForTestUrl(u.testUrl) <= bestDelay+u.tolerance {
			u.fastReason = fixedReason
			if u.fastReason == "" {
				if bestDelay < u.fastNode.LastDelayForTestUrl(u.testUrl) {
					u.fastReason = NowReasonTolerance
				} else {
					u.fastReason = NowReasonBest
				}
			}
			return u.fastNode, exhausted, nil
		}
		// A member the pool never measured (0xffff is not a delay but the
		// absence of one) is not faster on paper: its place among the other
		// unmeasured members is list order, and there is no measurement a
		// probe could make fresh.
		if bestDelay != 0xffff && !u.promotionFreshLocked(best.Name(), now) {
			// The candidate is faster on paper only: its measurement is older
			// than urlTestPoolPromotionFreshness, so the member may already be
			// gone, and a dial that leaves on it pays the outage the memory
			// hides. The pool keeps the member that is up and has the candidate
			// probed before it takes the dial.
			u.fastReason = fixedReason
			if u.fastReason == "" {
				u.fastReason = NowReasonPromotion
			}
			return u.fastNode, exhausted, best
		}
	}

	u.fastNode = best
	if fixedReason != "" {
		u.fastReason = fixedReason
	} else {
		u.fastReason = NowReasonBest
	}
	return best, exhausted, nil
}

// promotionFreshLocked reports whether the pool measured the member recently
// enough to promote it over the member a dial is already using.
func (u *URLTestPool) promotionFreshLocked(name string, now time.Time) bool {
	at, ok := u.measuredAt[name]
	return ok && now.Sub(at) < urlTestPoolPromotionFreshness
}

// startPromotionProbe measures one candidate before the pool promotes it. The
// probe is the same question a sweep asks — same URL, expected statuses and
// budget — so a check already measuring that member hands its probe over
// instead of loading the path twice, and only an answer stamps the measurement
// as fresh. A failed probe writes nothing: the proxy's own bookkeeping marks it
// not alive, and the next dial picks another candidate.
//
// It runs on its own goroutine: the queue measures a member that is itself a
// group inline on the caller's goroutine, and a dial must not wait for that.
func (u *URLTestPool) startPromotionProbe(proxy C.Proxy) {
	probe := probequeue.Probe{
		Proxy:    proxy,
		URL:      u.testUrl,
		Expected: u.expectedRanges,
		Timeout:  u.probeTimeoutFor(proxy, time.Duration(u.testTimeout)*time.Millisecond),
	}
	submit := probequeue.Submit
	if !u.lazy {
		submit = probequeue.SubmitForeground
	}
	go submit(u.Name(), []probequeue.Probe{probe}, func(result probequeue.Result) {
		if result.Err != nil {
			return
		}
		now := time.Now()
		u.mu.Lock()
		if u.measuredAt == nil {
			u.measuredAt = make(map[string]time.Time)
		}
		u.measuredAt[result.Name] = now
		u.mu.Unlock()
	})
}

func (u *URLTestPool) poolProxiesLocked(proxies []C.Proxy) []C.Proxy {
	pool, _ := proxiesByNames(proxies, u.pool)
	return pool
}

// viewLocked returns the pool resolved against the proxy list, rebuilding it
// when the list generation or the pool changed. It must be called with u.mu
// held: it reads u.pool.
func (u *URLTestPool) viewLocked(proxies []C.Proxy, listGen uint64) *poolView {
	sig := poolSignature(u.pool)
	if v := u.view.Load(); v != nil && v.listGen == listGen && v.poolSig == sig {
		return v
	}

	v := u.buildViewLocked(proxies, listGen, sig)
	u.view.Store(v)
	return v
}

// viewForLocked builds the view without publishing it: a caller that works on
// a list of its own must not poison the cache of the group. It must be called
// with u.mu held.
func (u *URLTestPool) viewForLocked(proxies []C.Proxy) *poolView {
	return u.buildViewLocked(proxies, 0, 0)
}

func (u *URLTestPool) buildViewLocked(proxies []C.Proxy, listGen, poolSig uint64) *poolView {
	v := &poolView{listGen: listGen, poolSig: poolSig}
	v.members, _ = proxiesByNames(proxies, u.pool)
	return v
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
		// measured order must not be overridden. An empty pool is the same
		// question even after a full check: the check measured nothing, so the
		// best reputation is the least blind guess the pool can make.
		if delay == bestDelay && delay == 0xffff &&
			(len(u.pool) == 0 || !u.hasFullCheck.Load()) &&
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
	if until, ok := u.cutMarks[name]; ok && now.Before(until) {
		return true
	}
	mark, ok := u.dataMarks[name]
	return ok && now.Before(mark.until)
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
// once instead of sitting out the whole interval. A cut is not a failure of the
// node, so it does not count toward the group health check either.
func (u *URLTestPool) onProxyCut(proxy C.Proxy, err error) {
	if !shouldTrackDialFailure(proxy.Type(), err) {
		return
	}
	u.markCut(proxy.Name())
	u.noteCutStorm(proxy.Name(), time.Now())
}

// noteCutStorm starts a quick check when several different members are cut by
// our own attempt-timeout within urlTestPoolStormWindow. One cut may be one
// slow node; a storm of them says the route the whole pool goes through is
// slow, and a check is what tells the pool which members still answer. The
// check is throttled, so a route that stays slow is not swept behind every
// connection, and a repeated cut of the same member is not a storm.
func (u *URLTestPool) noteCutStorm(name string, now time.Time) {
	u.mu.Lock()
	kept := u.cutStorm[:0]
	for _, cut := range u.cutStorm {
		if cut.name != name && now.Sub(cut.at) <= urlTestPoolStormWindow {
			kept = append(kept, cut)
		}
	}
	kept = append(kept, poolCut{name: name, at: now})
	if len(kept) > urlTestPoolStormCuts*2 {
		kept = kept[len(kept)-urlTestPoolStormCuts*2:]
	}
	u.cutStorm = kept
	storm := len(kept) >= urlTestPoolStormCuts &&
		(u.stormChecked.IsZero() || now.Sub(u.stormChecked) >= urlTestPoolStormThrottle)
	if storm {
		u.stormChecked = now
	}
	u.mu.Unlock()

	if !storm {
		return
	}
	log.Debugln("ProxyGroup: %s: attempt-timeout cut %d members within %s, checking the pool early", u.Name(), len(kept), urlTestPoolStormWindow)
	u.scheduleCheck(poolQuickCheck)
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
	if !u.markFor(&u.suspicious, name, ttl) {
		return
	}
	// One event per outage: a member that keeps failing only extends its mark,
	// and the history must not turn a single dead node into a wall of rows.
	RecordGroupEvent(GroupEvent{
		Group:  groupName(u.GroupBase),
		Type:   GroupEventSuspicious,
		To:     name,
		Detail: "for " + ttl.String(),
	})
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
func (u *URLTestPool) markFor(marks *map[string]time.Time, name string, ttl time.Duration) bool {
	u.mu.Lock()
	if *marks == nil {
		*marks = make(map[string]time.Time)
	}
	now := time.Now()
	next := now.Add(ttl)
	previous, existed := (*marks)[name]
	// A mark that had already expired is a new outage, not a repeat of the
	// one the history already names.
	fresh := !existed || !now.Before(previous)
	if !existed || previous.Before(next) {
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
	return fresh
}

func (u *URLTestPool) maybeScheduleCheck() {
	if u.dropBackoffOnNetworkChange() {
		// Every member measured on the old route says nothing about the new
		// one, so the first traffic after the device moved starts a fresh
		// sweep at once instead of waiting out the schedule.
		u.scheduleCheck(poolFullCheck)
		return
	}
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

// dropBackoffOnNetworkChange ends the outage backoff and drops the marks when
// the device moved to another network: a member measured dead, or cut by an
// attempt-timeout, on the old network is not a verdict on the new one. It
// reports the change, because the pool itself has nothing measured for the new
// route and its caller has to start a check instead of waiting for the
// schedule of the old one.
func (u *URLTestPool) dropBackoffOnNetworkChange() bool {
	generation := iface.NetworkGeneration()
	u.mu.RLock()
	seen := u.networkGen
	u.mu.RUnlock()
	if generation == seen {
		return false
	}

	u.mu.Lock()
	u.networkGen = generation
	u.suspicious = nil
	u.cutMarks = nil
	// a member that stayed silent on the old route is not a verdict on the
	// new one either: the same node often works fine after the device moves
	u.dataMarks = nil
	// the freshness of a promotion measures the old route too: a member
	// measured a minute ago on another network is not proven on this one, and
	// promoting it straight from memory is the same blind switch the marks are
	// cleared to avoid
	u.measuredAt = nil
	u.mu.Unlock()
	u.exitBackoff("the network changed")
	return true
}

// scheduledCheckGeneration captures the network generation a scheduled check
// runs under and reports whether its failed probes are worth one retry: the
// first check after the device moved to another network sees timeouts the old
// route caused, not a verdict on the members.
func (u *URLTestPool) scheduledCheckGeneration() (uint64, bool) {
	generation := iface.NetworkGeneration()
	u.mu.RLock()
	seen := u.lastCheckGen
	u.mu.RUnlock()
	return generation, generation != seen
}

// finishScheduledCheck records the generation the check measured under. A
// network change that happened while the check ran stays a change: the next
// check still retries, because the probes of this one went out on the old
// route.
func (u *URLTestPool) finishScheduledCheck(generation uint64) {
	u.mu.Lock()
	u.lastCheckGen = generation
	u.mu.Unlock()
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
	if u.dropBackoffOnNetworkChange() {
		// The throttle measures the old route: a pool that just lost its
		// network has to be measured again, not waited for.
		u.scheduleCheck(poolFullCheck)
		return
	}
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
		generation := u.checkGen
		u.mu.RUnlock()
		if kind == poolFullCheck && (done == nil || generation != iface.NetworkGeneration()) {
			// The running check measured the old route, and waiting out its
			// schedule would leave the pool unproven on the new one: the
			// check re-arms the sweep as it ends.
			u.pendingFull.Store(true)
		}
		return done, false
	}

	channel := make(chan struct{})
	u.mu.Lock()
	u.checkDone = channel
	u.checkResults = nil
	u.checkGen = iface.NetworkGeneration()
	u.mu.Unlock()

	if kind == poolFullCheck || kind == poolWarmCheck {
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

			if u.pendingFull.Swap(false) {
				// A sweep of the new route was asked for while this check
				// measured the old one: run it now instead of leaving the
				// pool unproven until the next schedule.
				u.scheduleCheck(poolFullCheck)
			}
		}()
		defer u.finishWarm()
		switch kind {
		case poolFullCheck:
			u.fullCheck()
		case poolWarmCheck:
			u.warmCheck()
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

// beginWarmup opens the channels a cold-start dial waits on: warmCh releases a
// wait that only needs the first measured member, warmFill releases the dial
// wait once the sweep answered for a sample of the pool.
func (u *URLTestPool) beginWarmup() {
	u.mu.Lock()
	if u.warmCh == nil {
		u.warmCh = make(chan struct{})
		u.warmFill = make(chan struct{})
		u.warmDelivered = 0
		u.checkStart = time.Now()
	}
	u.mu.Unlock()
}

// markWarm releases the waits that only need the first measured member of the
// running check: a dial of an exhausted pool must not spend its budget on a
// node the check already knows is dead.
func (u *URLTestPool) markWarm() {
	u.mu.Lock()
	if u.warmCh != nil {
		close(u.warmCh)
		u.warmCh = nil
	}
	u.mu.Unlock()
}

// finishWarm releases every wait of the check: it ended, and a sweep that
// measured nothing alive must not leave the first dials waiting out the window.
func (u *URLTestPool) finishWarm() {
	u.mu.Lock()
	if u.warmCh != nil {
		close(u.warmCh)
		u.warmCh = nil
	}
	if u.warmFill != nil {
		close(u.warmFill)
		u.warmFill = nil
	}
	u.mu.Unlock()
}

// noteWarmResult books one answered probe of a check. The fill gate closes
// once the sweep answered for as many members as the pool wants to hold: the
// dial leaves on the best of the sample instead of waiting for the whole list,
// and a sample without a single alive member keeps the gate open for the rest.
func (u *URLTestPool) noteWarmResult(target, delivered, alive int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if delivered > u.warmDelivered {
		u.warmDelivered = delivered
	}
	if u.warmFill == nil || alive <= 0 || delivered < target {
		return
	}
	close(u.warmFill)
	u.warmFill = nil
}

// waitWarm lets dials that arrive before the pool has a sample wait for one, so
// a connection leaves on the best of a measured sample instead of the first
// proxy of the list only because nothing has been measured yet. The wait ends
// as soon as the sweep answered for `pool-size` members with one alive
// (warmFill), when the check ends, on the window counted from the start of the
// check, or when half the window passed without a single answer — a check still
// queued behind the groups ahead of it must not hold the dial. The wait also
// never spends more than half of the caller's budget: the dial and its retries
// need the rest.
func (u *URLTestPool) waitWarm(ctx context.Context) {
	u.mu.RLock()
	cold := len(u.pool) == 0
	fixed := u.selected != ""
	channel := u.warmFill
	started := u.checkStart
	u.mu.RUnlock()
	if !cold || fixed || channel == nil {
		return
	}

	// A dial needs this pool now: its scheduled check takes the head of the
	// ordered chain instead of waiting for the waves ahead of it. The request
	// is remembered if the check has not reached the queue yet.
	probequeue.Promote(u.Name())

	if u.firstWaveWait <= 0 {
		return
	}

	wait := u.firstWaveWait - time.Since(started)
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining/2 < wait {
			wait = remaining / 2
		}
	}
	if wait <= 0 {
		return
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	// A sweep that produced no answer by the middle of the window is stuck
	// behind the probes of another group: waiting out the rest of the window
	// only delays the connection.
	half := time.NewTimer(wait / 2)
	defer half.Stop()
	for {
		select {
		case <-channel:
			return
		case <-timer.C:
			return
		case <-ctx.Done():
			return
		case <-half.C:
			u.mu.RLock()
			delivered := u.warmDelivered
			u.mu.RUnlock()
			if delivered == 0 {
				return
			}
		}
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
// per change instead of a poll every interval. A `lazy` group keeps its checks
// driven by real traffic and is not swept here, but it still measures the
// members the reputation trusts: that is the handful of probes the first dial
// would spend anyway, and it takes the wait away from it.
func (u *URLTestPool) WarmUp() {
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
		if u.lazy {
			u.scheduleCheck(poolWarmCheck)
			return
		}
		u.maybeScheduleCheck()
	}()
}

// warmCheck fills a lazy pool before the first dial: it measures the members
// the reputation trusts instead of sweeping every candidate of the providers.
// A group that asked for checks driven by traffic otherwise pays for them with
// its first connection, and probing the leaders costs the pool size instead.
// The result goes through applyResults, so the pool starts with a real
// measurement and the first dial neither waits for one nor starts a sweep of
// its own.
func (u *URLTestPool) warmCheck() {
	proxies := u.GetProxies(true)
	if !u.proxiesReady(proxies) {
		return
	}

	candidates := u.reputationCandidates(proxies, u.poolSize)
	if len(candidates) == 0 {
		return
	}

	log.Infoln("ProxyGroup: %s start smart warm-up of %d candidates", u.Name(), len(candidates))
	results, _ := u.probeProxies(context.Background(), candidates, time.Duration(u.testTimeout)*time.Millisecond, true, false, false)
	if len(results) == 0 {
		// a warm-up that measured nothing says nothing about the pool: the
		// device may still be bringing its network up, and the first dial has
		// to find its own answer instead of inheriting an empty-sweep backoff
		log.Infoln("ProxyGroup: %s smart warm-up measured nothing, the first dial takes over", u.Name())
		return
	}
	u.storeResults(results)
	u.applyResults(results)
}

// reputationCandidates lists the candidates a warm-up probes: the members with
// the best reputation, in list order among equals, so a profile that has never
// been measured still starts from the first proxies of its subscription. A
// member the reputation sank below the suspect band is left to the sweep: the
// warm-up exists to prepare a working pool, not to retry what already failed.
func (u *URLTestPool) reputationCandidates(proxies []C.Proxy, limit int) []C.Proxy {
	if limit <= 0 {
		return nil
	}

	type warmCandidate struct {
		proxy C.Proxy
		score int
	}
	items := make([]warmCandidate, 0, len(proxies))
	for _, proxy := range proxies {
		items = append(items, warmCandidate{proxy: proxy, score: u.reputation.Score(proxy.Name())})
	}
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].score > items[j].score
	})

	picked := make([]C.Proxy, 0, limit)
	for _, item := range items {
		if len(picked) == limit {
			break
		}
		if item.score <= reputation.Suspect {
			continue
		}
		picked = append(picked, item.proxy)
	}
	return picked
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
	generation, retry := u.scheduledCheckGeneration()
	results, _ := u.testProxiesStreaming(context.Background(), poolProxies, retry)
	u.finishScheduledCheck(generation)
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
	results, _ := u.probeProxies(ctx, candidates, timeout, true, true, false)
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

// noteDialSucceeded remembers the proxy that carried traffic: it ends the
// outage, feeds the reputation and clears the marks a failure earned. It runs
// only on evidence of traffic: a dial that merely accepted the connection
// proves the route works, not that the node carries the flow.
func (u *URLTestPool) noteDialSucceeded(proxy C.Proxy) {
	if tracksNodeReputation(proxy.Type()) {
		u.reputation.DialSucceeded(proxy.Name())
	}

	u.mu.Lock()
	// a member that carried a connection is not suspicious: a mark earned on
	// another route, or by a sweep that measured nothing at all, must not
	// outlive the proof that this one works
	until, marked := u.suspicious[proxy.Name()]
	recovered := marked && time.Now().Before(until)
	delete(u.suspicious, proxy.Name())
	delete(u.cutMarks, proxy.Name())
	u.mu.Unlock()
	if recovered {
		RecordGroupEvent(GroupEvent{
			Group:  groupName(u.GroupBase),
			Type:   GroupEventRecovered,
			To:     proxy.Name(),
			Detail: "carried traffic",
		})
	}

	u.noteDialAccepted(proxy)
}

// noteDialAccepted remembers the route the dial proved and rebuilds an empty
// pool (for example after a full outage). It is the part of noteDialSucceeded
// that does not depend on the member carrying traffic yet: a protocol whose
// handshake the first byte will prove still tells us the network works, so the
// route bookkeeping must not wait for it.
func (u *URLTestPool) noteDialAccepted(proxy C.Proxy) {
	// the member carried traffic, so the outage is over: the next full sweep
	// follows the usual schedule again instead of the empty-streak backoff
	u.exitBackoff("a dial succeeded")

	u.mu.Lock()
	u.lastGood = proxy.Name()
	empty := len(u.pool) == 0
	if u.measuredAt == nil {
		u.measuredAt = make(map[string]time.Time)
	}
	// the member carried a dial, so it is alive as of now: that is the
	// freshest evidence the pool has and the one a promotion may rely on
	u.measuredAt[proxy.Name()] = time.Now()
	u.mu.Unlock()
	if !empty {
		return
	}

	if last, ok := u.lastFull.LoadOk(); ok && time.Since(last) < urlTestPoolRecoveryCooldown {
		return
	}
	u.scheduleCheck(poolFullCheck)
}

// observeDataPlane installs the data-plane observer of a fresh connection. The
// observer catches what the dial cannot see: a member that accepted the
// handshake and then went silent, reset the flow, or ended it without an
// answer. The watchdog is sized by the answer time measured for the member.
// creditFirstByte marks a protocol whose dial says nothing about the carrier:
// its reputation is credited at the first answered byte.
func (u *URLTestPool) observeDataPlane(proxy C.Proxy, c C.Conn, creditFirstByte bool) C.Conn {
	if callback.IsDataPlaneObserved(c) {
		return c
	}

	return callback.NewDataPlaneConn(c, callback.DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: u.firstByteWatchdog(proxy.Name()),
		OnFirstByte: func(delay time.Duration) {
			u.noteDataAnswered(proxy, delay)
			if creditFirstByte {
				u.noteDialSucceeded(proxy)
			}
		},
		OnDrop: func(err error) {
			u.onDataSilent(proxy, err)
		},
	})
}

// noteDataAnswered records a member that carried a real answer: the proof
// clears the silent mark at once, resets its strike count and feeds the
// watchdog baseline of the next connection.
func (u *URLTestPool) noteDataAnswered(proxy C.Proxy, delay time.Duration) {
	name := proxy.Name()
	now := time.Now()

	u.mu.Lock()
	previousMark, marked := u.dataMarks[name]
	recovered := marked && now.Before(previousMark.until)
	delete(u.dataMarks, name)
	if u.firstBytes == nil {
		u.firstBytes = make(map[string]*poolFirstByte)
	}
	sample := u.firstBytes[name]
	if sample == nil {
		sample = &poolFirstByte{}
		u.firstBytes[name] = sample
	}
	if sample.hits == 0 {
		sample.avg = delay
	} else {
		sample.avg += (delay - sample.avg) / urlTestPoolFirstByteWeight
	}
	if sample.hits < 1000 {
		sample.hits++
	}
	sample.last = now
	u.pruneDataEvidenceLocked(now)
	u.mu.Unlock()
	if recovered {
		RecordGroupEvent(GroupEvent{
			Group:  groupName(u.GroupBase),
			Type:   GroupEventRecovered,
			To:     name,
			Detail: "answered again",
		})
	}
}

// firstByteWatchdog sizes the reply watchdog of the next connection: never
// below the floor, scaled by what the member usually takes to answer, never
// above the cap.
func (u *URLTestPool) firstByteWatchdog(name string) time.Duration {
	watch := urlTestPoolFirstByteFloor

	u.mu.RLock()
	sample := u.firstBytes[name]
	if sample != nil && sample.hits > 0 {
		if scaled := sample.avg * urlTestPoolFirstByteFactor; scaled > watch {
			watch = scaled
		}
	}
	u.mu.RUnlock()

	if watch > urlTestPoolFirstByteCap {
		watch = urlTestPoolFirstByteCap
	}
	return watch
}

// onDataSilent records a member that carried the handshake and then went
// silent. That is the failure a dial cannot see: the node accepted the
// connection, so the dial reported success, and the user was left waiting on
// the flow. The first strike keeps the member out for one reconnect only; a
// member that keeps dying after the handshake doubles its demotion up to the
// cap, and a real answered byte clears the evidence at once.
func (u *URLTestPool) onDataSilent(proxy C.Proxy, err error) {
	if !shouldTrackDialFailure(proxy.Type(), err) {
		return
	}

	name := proxy.Name()
	now := time.Now()

	u.mu.Lock()
	if u.dataMarks == nil {
		u.dataMarks = make(map[string]*poolDataMark)
	}
	mark := u.dataMarks[name]
	if mark == nil {
		mark = &poolDataMark{}
		u.dataMarks[name] = mark
	}
	if now.Sub(mark.last) > urlTestPoolDataStrikeWindow {
		mark.strikes = 0
	}
	// The flows of one incident die together: the first silent flow starts the
	// strike, and the ones that follow it within the dedupe window only extend
	// the demotion. Without this, a burst of parallel connections turns one
	// silent node into the fastest possible escalation within a second.
	if mark.strikes == 0 || now.Sub(mark.last) >= urlTestPoolStrikeDedupe {
		mark.strikes++
	}
	mark.last = now
	// The history names the first silent flow of an incident, not every flow
	// that followed it: a burst of parallel connections dies together.
	notify := !now.Before(mark.until)

	ttl := urlTestPoolDataStrike
	for strikes := mark.strikes; strikes > 1 && ttl < urlTestPoolDataStrikeMax; strikes-- {
		ttl *= 2
	}
	if ttl > urlTestPoolDataStrikeMax {
		ttl = urlTestPoolDataStrikeMax
	}
	if until := now.Add(ttl); until.After(mark.until) {
		mark.until = until
	}
	strikes := mark.strikes
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
	u.pruneDataEvidenceLocked(now)
	u.mu.Unlock()

	log.Warnln("ProxyGroup: %s: %s accepted the handshake and went silent (%v), keep it out for %s", u.Name(), name, err, ttl)
	if notify {
		RecordGroupEvent(GroupEvent{
			Group:  groupName(u.GroupBase),
			Type:   GroupEventSilent,
			To:     name,
			Detail: "for " + ttl.String(),
		})
	}
	if strikes > 1 && tracksNodeReputation(proxy.Type()) {
		u.reputation.DataFailed(name)
	}
	if exhausted {
		// every member of the pool is out of the next choices: the emergency
		// test and a fresh sweep are the only ways to find one that still
		// carries the flow the probes alone could not see
		u.requestFullCheckOnExhaustion()
	}
}

// pruneDataEvidenceLocked bounds the per-member evidence maps: a subscription
// that rotates servers must not grow them for the whole session.
func (u *URLTestPool) pruneDataEvidenceLocked(now time.Time) {
	if len(u.dataMarks) > urlTestPoolDataEvidenceCap {
		for name, mark := range u.dataMarks {
			if now.Sub(mark.last) > urlTestPoolDataStrikeWindow {
				delete(u.dataMarks, name)
			}
		}
	}
	if len(u.firstBytes) > urlTestPoolDataEvidenceCap {
		for name, sample := range u.firstBytes {
			if now.Sub(sample.last) > urlTestPoolDataStrikeWindow {
				delete(u.firstBytes, name)
			}
		}
	}
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
		results, tested := u.probeProxies(context.Background(), candidates, urlTestPoolRecoveryTestTimeout, true, true, false)
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
		// Every miss moves the window on: the head of the order is
		// deterministic, so an outage that killed those members would replay
		// the same window of the order forever and never reach the rest of
		// the list. The first scan keeps the head; each further miss starts
		// one window later, wrapping around.
		offset := 0
		if misses > 1 {
			offset = ((misses - 1) * urlTestPoolRecoveryScanLimit) % len(scan)
		}
		if offset > 0 {
			window := make([]C.Proxy, 0, urlTestPoolRecoveryScanLimit)
			window = append(window, scan[offset:]...)
			window = append(window, scan[:offset]...)
			if len(window) > urlTestPoolRecoveryScanLimit {
				window = window[:urlTestPoolRecoveryScanLimit]
			}
			scan = window
		} else {
			scan = scan[:urlTestPoolRecoveryScanLimit]
		}
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
	u.finishWarm()
}

func (u *URLTestPool) fullCheck() {
	proxies := u.GetProxies(true)
	if !u.proxiesReady(proxies) {
		return
	}

	log.Debugln("ProxyGroup: %s start full health check for %d proxies", u.Name(), len(proxies))
	// Publish every measured member as it arrives, so a cold pool routes by a
	// real delay from the first result instead of waiting for the whole list.
	generation, retry := u.scheduledCheckGeneration()
	results, _ := u.testProxiesStreaming(context.Background(), u.orderForCheck(proxies), retry)
	u.finishScheduledCheck(generation)
	u.storeResults(results)
	u.applyResults(results)
}

// orderForCheck puts the members that proved reliable first, ordered by their
// measured delay inside a tier, so the first wave of a sweep is the one
// likeliest to find an alive member. A pool that lost its route is then
// refilled from the first results instead of after every candidate of a large
// provider was probed, which is what made an outage look like a long search
// for a node. The members the pool marked as out of play go behind the rest,
// because those marks are what a route that just died leaves behind: a sweep
// that starts with them spends its first wave on the nodes the previous one
// already measured dead, and the transport coverage keeps the wave from being
// filled by one transport of a mixed subscription.
//
// The reputation orders the tiers before the delay: a node that carried
// traffic recently is likelier to answer than one that keeps failing, and a
// cold sweep has one budget to spend on the nodes worth a probe. A fast but
// unreliable node is still measured in the same sweep; it only may wait one
// wave behind the trusted ones, and once the pool is rebuilt the delay decides
// the routing again.
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
	// The marks, the delays and the scores are read once instead of inside the
	// comparator: a sweep of a large provider must not take the pool lock, the
	// proxy state or the store lock for every comparison.
	type checkOrder struct {
		proxy  C.Proxy
		marked bool
		tier   int
		band   int
		delay  uint16
		score  int
	}
	items := make([]checkOrder, len(ordered))
	now := time.Now()
	u.mu.RLock()
	for i, proxy := range ordered {
		items[i] = checkOrder{
			proxy:  proxy,
			marked: !u.markUntilLocked(proxy.Name(), now).IsZero(),
		}
	}
	u.mu.RUnlock()
	for i := range items {
		items[i].delay = items[i].proxy.LastDelayForTestUrl(u.testUrl)
		items[i].score = u.reputation.Score(items[i].proxy.Name())
		items[i].band = (items[i].score + 100) / urlTestPoolReputationBand
		switch {
		case items[i].score >= reputation.Trusted:
			items[i].tier = 0
		case items[i].score >= reputation.Suspect:
			items[i].tier = 1
		default:
			items[i].tier = 2
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].marked != items[j].marked {
			return !items[i].marked
		}
		if items[i].tier != items[j].tier {
			return items[i].tier < items[j].tier
		}
		// The reputation leads inside a tier: a node that carried traffic
		// recently is likelier to answer than one that keeps failing, and the
		// first wave has one budget to spend. A band of ten points absorbs the
		// noise of a single strike, so one bad flow does not reorder the wave,
		// and the delay decides between the members of one band.
		if items[i].band != items[j].band {
			return items[i].band > items[j].band
		}
		if items[i].delay != items[j].delay {
			return items[i].delay < items[j].delay
		}
		// Every member that is not measured alive right now shares the max
		// delay, so the reputation decides which of them is worth probing
		// first: a node that carried traffic before is likelier to come back
		// than one that has been failing for days.
		if items[i].score != items[j].score {
			return items[i].score > items[j].score
		}
		return items[i].proxy.Name() < items[j].proxy.Name()
	})
	for i := range items {
		ordered[i] = items[i].proxy
	}
	// The transport coverage only looks at the members a sweep may move into
	// its first wave: the unmarked nodes of the trusted and the ordinary
	// tiers. A marked node or one below the suspect band stays where the sort
	// put it, however unique its transport is.
	eligible := 0
	for _, item := range items {
		if item.marked || item.tier > 1 {
			break
		}
		eligible++
	}
	return u.coverTransports(ordered, eligible)
}

// coverTransports moves one member of every transport the head of a sweep does
// not have into that head. A subscription that lists dozens of hysteria2 nodes
// and a couple of vless ones hides the survivors of a QUIC outage behind the
// whole first wave: the wave spends its budget on one transport while the node
// that answers is still queued. The promoted members keep their relative
// order and the rest of the sweep keeps its own, so a sweep that already covers
// every transport of its head is left untouched.
//
// Only the first `eligible` members are candidates for promotion: those are the
// unmarked nodes of the trusted and ordinary tiers. A marked node, or one the
// reputation already sunk, is never pulled into the first wave by its transport
// type alone — the wave would spend its budget on the same nodes the sort just
// moved to the back.
func (u *URLTestPool) coverTransports(ordered []C.Proxy, eligible int) []C.Proxy {
	wave := probequeue.GetConcurrency()
	if wave <= 0 || len(ordered) <= wave || eligible <= wave {
		return ordered
	}

	// a subscription mixes a handful of transports, so a small slice beats a
	// map on both the lookup and the allocation
	present := make([]C.AdapterType, 0, 4+urlTestPoolFirstWaveCoverage)
	hasType := func(adapterType C.AdapterType) bool {
		for _, seen := range present {
			if seen == adapterType {
				return true
			}
		}
		return false
	}
	for _, proxy := range ordered[:wave] {
		if !hasType(proxy.Type()) {
			present = append(present, proxy.Type())
		}
	}

	var promoted [urlTestPoolFirstWaveCoverage]C.Proxy
	count := 0
	for _, proxy := range ordered[wave:eligible] {
		if count == urlTestPoolFirstWaveCoverage {
			break
		}
		if hasType(proxy.Type()) {
			continue
		}
		present = append(present, proxy.Type())
		promoted[count] = proxy
		count++
	}
	if count == 0 {
		return ordered
	}

	covered := make([]C.Proxy, len(ordered))
	copy(covered, promoted[:count])
	next := count
	for _, proxy := range ordered {
		if containsProxy(promoted[:count], proxy) {
			continue
		}
		covered[next] = proxy
		next++
	}
	return covered
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
	u.finishWarm()
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
// Only an alive result releases the check wait: a probe that failed is not a
// member a dial can leave on, and the wait must cover the wave that may still
// find one. The fill gate of a cold-start dial is not released here: the pool
// takes the best of a sample, so one alive member is not the sample yet.
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
	previous := u.pool
	if u.measuredAt == nil {
		u.measuredAt = make(map[string]time.Time, len(results))
	}
	for name := range results {
		u.measuredAt[name] = now
	}
	if len(u.measuredAt) > urlTestPoolDataEvidenceCap {
		for name, at := range u.measuredAt {
			if now.Sub(at) > urlTestPoolDataStrikeWindow {
				delete(u.measuredAt, name)
			}
		}
	}
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

	// Only a finished check reports the pool: a partial publish runs for every
	// probe, and the order follows the measured delay, so the same members
	// measured again are not a change of the pool.
	if final && !sameNameSet(previous, names) {
		RecordGroupEvent(GroupEvent{
			Group:  groupName(u.GroupBase),
			Type:   GroupEventPool,
			Detail: fmt.Sprintf("%d -> %d members", len(previous), len(names)),
		})
	}
}

// sameNameSet reports whether two member lists hold the same names. The pool
// order follows the measured delay, so a rebuild of the same members is not a
// change of the pool.
func sameNameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]struct{}, len(a))
	for _, name := range a {
		seen[name] = struct{}{}
	}
	for _, name := range b {
		if _, ok := seen[name]; !ok {
			return false
		}
	}
	return true
}

// testProxiesStreaming sweeps proxies with the member timeout and publishes
// every measured member as soon as its probe returns. With retry set, the
// probes that fail get one second attempt after a short pause, so the first
// check on a new network does not publish the timeouts the old route caused.
func (u *URLTestPool) testProxiesStreaming(ctx context.Context, proxies []C.Proxy, retry bool) (map[string]uint16, map[string]struct{}) {
	return u.probeProxies(ctx, proxies, time.Duration(u.testTimeout)*time.Millisecond, true, false, retry)
}

// probeSubmit chooses the lane of one wave. A probe a caller waits on carries
// Priority itself, which is what puts it in the priority lane, so the batch is
// submitted as usual. A cold pool goes to the ordered chain: nothing is
// measured yet, so a dial waits for the first answer, and each wave of the
// profile runs in the order it set instead of all of them at once. A dial that
// arrives for the pool promotes its check to the head of the chain. A warm
// pool keeps checking by its `lazy`: a group the profile routes by its own
// answers runs in the foreground lane, a lazy one takes the background ring.
//
// The retry wave asks this again: the first wave may have measured the member
// that makes the pool warm, and its failures then belong in the warm lane.
func (u *URLTestPool) probeSubmit(proxies []C.Proxy, priority bool) func(string, []probequeue.Probe, func(probequeue.Result)) *probequeue.Check {
	if priority {
		return probequeue.Submit
	}
	if !u.hasUsableNode(proxies) {
		foreground := !u.lazy
		return func(source string, probes []probequeue.Probe, onResult func(probequeue.Result)) *probequeue.Check {
			return probequeue.SubmitChain(source, foreground, probes, onResult)
		}
	}
	if !u.lazy {
		return probequeue.SubmitForeground
	}
	return probequeue.Submit
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
func (u *URLTestPool) probeProxies(ctx context.Context, proxies []C.Proxy, timeout time.Duration, publish bool, priority bool, retry bool) (map[string]uint16, map[string]struct{}) {
	// Every pool check, from the scheduled quick sweep to the recovery
	// measurement, funnels its probes through here, so this is the one place
	// that knows the whole set before the first probe answers.
	endCheck := beginCheckScope(u.Name(), u.testUrl, proxies)
	defer endCheck()

	results := make(map[string]uint16, len(proxies))
	tested := make(map[string]struct{}, len(proxies))
	// failed collects the members a wave measured dead. The reputation takes
	// them only once the wave measured at least one alive member: when the
	// whole candidate list times out, the route is down and the verdict
	// belongs to the network, not to every node the wave reached.
	failed := make([]string, 0, len(proxies))
	var mu sync.Mutex

	probes := u.probesFor(proxies, timeout, priority)
	submit := u.probeSubmit(proxies, priority)
	// The sample a cold-start dial waits for: the pool holds no more members
	// than its size, and no more than the sweep was given.
	target := u.poolSize
	if target <= 0 || target > len(proxies) {
		target = len(proxies)
	}
	deliver := func(result probequeue.Result, recordFailure bool) {
		mu.Lock()
		defer mu.Unlock()

		tested[result.Name] = struct{}{}
		if result.Err != nil {
			if publish {
				// the failed member leaves the pool now instead of
				// holding a slot until the check ends
				u.publishPartial(results, tested)
				u.noteWarmResult(target, len(tested), len(results))
			}
			if recordFailure && !errors.Is(result.Err, context.Canceled) {
				failed = append(failed, result.Name)
			}
			return
		}

		results[result.Name] = result.Delay
		if publish {
			// published under the results lock: a later publish always
			// carries every earlier result, so the pool cannot shrink back
			u.publishPartial(results, tested)
			u.noteWarmResult(target, len(tested), len(results))
		}
	}
	// A retried check leaves the reputation to its second wave: a timeout on a
	// network that is still settling is not a verdict, and only a member that
	// failed both attempts has earned the failure.
	check := submit(u.Name(), probes, func(result probequeue.Result) {
		deliver(result, !retry)
	})
	check.Wait(ctx)

	if retry && ctx.Err() == nil {
		mu.Lock()
		failed := failedProbes(proxies, results, tested)
		mu.Unlock()
		if len(failed) > 0 {
			pause := time.NewTimer(urlTestPoolNetworkRetryPause)
			select {
			case <-pause.C:
				// The second wave decides its lane again: the first one may
				// have measured the member that made the pool warm.
				retrySubmit := u.probeSubmit(failed, priority)
				retryCheck := retrySubmit(u.Name(), u.probesFor(failed, timeout, priority), func(result probequeue.Result) {
					deliver(result, true)
				})
				retryCheck.Wait(ctx)
			case <-ctx.Done():
				pause.Stop()
			}
		}
	}

	// The answer is a snapshot: a check the caller left behind keeps probing
	// and keeps publishing through the callback above, but it must not write
	// the maps this caller reads.
	mu.Lock()
	pending := append([]string(nil), failed...)
	snapshot := copyDelayResults(results)
	testedNames := copyTestedNames(tested)
	mu.Unlock()

	u.recordProbeFailures(snapshot, pending, timeout)
	return snapshot, testedNames
}

// recordProbeFailures charges the members a check measured dead. A wave that
// measured nothing alive is not a verdict on the members: an outage takes the
// whole candidate list down at once, and charging every node for it would sink
// a node that worked for days after one dead sweep.
//
// A member the pool already marked is left alone when the wave probed it with
// the short budget: that failure says no more than the mark already does, and
// charging it again would sink a slow but working node twice for one outage.
func (u *URLTestPool) recordProbeFailures(results map[string]uint16, failed []string, timeout time.Duration) {
	if len(results) == 0 || len(failed) == 0 {
		return
	}

	// only a shortened wave needs the marks: a member probed with its own
	// timeout is measured exactly as before
	var marked map[string]struct{}
	if timeout > urlTestPoolMarkedProbeTimeout {
		now := time.Now()
		marked = make(map[string]struct{}, len(failed))
		u.mu.RLock()
		for _, name := range failed {
			if !u.failedUntilLocked(name, now).IsZero() {
				marked[name] = struct{}{}
			}
		}
		u.mu.RUnlock()
	}

	for _, name := range failed {
		if _, ok := marked[name]; ok {
			continue
		}
		u.reputation.ProbeFailed(name)
	}
}

// failedProbes lists the proxies a check reached and measured dead, so the
// retry wave asks exactly the questions the first one answered "no" to. A
// member the check never reached is not retried: its probe is still running or
// the caller left before it started.
func failedProbes(proxies []C.Proxy, results map[string]uint16, tested map[string]struct{}) []C.Proxy {
	failed := make([]C.Proxy, 0, len(proxies))
	for _, proxy := range proxies {
		if _, alive := results[proxy.Name()]; alive {
			continue
		}
		if _, reached := tested[proxy.Name()]; !reached {
			continue
		}
		failed = append(failed, proxy)
	}
	return failed
}

// probesFor builds the probes of one wave: the same URL and expected statuses
// for every member, so a retried probe asks the queue exactly the question the
// first wave asked and can share a probe that is still in flight. The budget
// stays the wave's own, except for the members the pool already marked as dead.
func (u *URLTestPool) probesFor(proxies []C.Proxy, timeout time.Duration, priority bool) []probequeue.Probe {
	probes := make([]probequeue.Probe, 0, len(proxies))
	for _, proxy := range proxies {
		probes = append(probes, probequeue.Probe{
			Proxy:    proxy,
			URL:      u.testUrl,
			Expected: u.expectedRanges,
			Timeout:  u.probeTimeoutFor(proxy, timeout),
			Priority: priority,
		})
	}
	return probes
}

// probeTimeoutFor shortens the budget of a member the pool already marked as
// out of play: a dial it failed or a flow that died after the handshake. One
// that answers now does so within the short budget, and the wave must not spend
// the whole member timeout on the nodes the previous sweep already measured
// dead while the unknowns wait behind them. A cut by our own attempt-timeout is
// not evidence about the node, so it keeps the member's own timeout.
func (u *URLTestPool) probeTimeoutFor(proxy C.Proxy, timeout time.Duration) time.Duration {
	if timeout <= urlTestPoolMarkedProbeTimeout {
		return timeout
	}
	now := time.Now()
	u.mu.RLock()
	marked := !u.failedUntilLocked(proxy.Name(), now).IsZero()
	u.mu.RUnlock()
	if !marked {
		return timeout
	}
	return urlTestPoolMarkedProbeTimeout
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

	firstWaveWait := defaultURLTestPoolFirstWaveWait
	if option.Lazy {
		firstWaveWait = defaultURLTestPoolLazyWaveWait
	}
	if poolOption.FirstWaveWait != nil {
		firstWaveWait = time.Duration(*poolOption.FirstWaveWait) * time.Millisecond
		if firstWaveWait < 0 {
			firstWaveWait = 0
		}
	}

	var ordered *int
	if poolOption.Ordered != nil {
		if *poolOption.Ordered < 0 {
			return nil, fmt.Errorf("ordered %d must be a non-negative integer", *poolOption.Ordered)
		}
		value := *poolOption.Ordered
		ordered = &value
	}

	testUrl := option.URL
	if testUrl == "" {
		testUrl = C.DefaultTestURL
	}

	base, err := NewGroupBase(GroupBaseOption{
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
	if err != nil {
		return nil, err
	}

	// A result cannot be published before one probe finishes, so the probe
	// timeout is the meaningful cap on the first-result wait of a pool that has
	// nothing measured alive.
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
		firstWaveWait:   firstWaveWait,
		attemptTimeout:  attemptTimeout,
		recoveryWait:    urlTestPoolRecoveryWait,
		lazy:            option.Lazy,
		ordered:         ordered,
		reputation:      reputation.Default(),
		preWarmRecheck:  urlTestPoolPreWarmRecheck,
		backoffJitter:   randomBackoffJitter,
		networkGen:      iface.NetworkGeneration(),
		lastCheckGen:    iface.NetworkGeneration(),
		// a member that failed a dial is skipped until the next quick check
		// could have re-measured it
		suspiciousTTL: interval,
		suspicious:    make(map[string]time.Time),
		cutMarks:      make(map[string]time.Time),
		dataMarks:     make(map[string]*poolDataMark),
		firstBytes:    make(map[string]*poolFirstByte),
	}, nil
}
