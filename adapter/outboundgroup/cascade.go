package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/outboundgroup/probequeue"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
)

const (
	defaultCascadeLookahead      = 2
	defaultCascadeInterval       = 300 * time.Second
	defaultCascadeFailedCooldown = 30 * time.Second
	// The periodic check probes until it finds `lookahead+1` alive candidates,
	// but never spends more than this many probes per cycle. That keeps the
	// check cost bounded on a large member list with many dead entries.
	cascadeProbeBudgetFactor = 2
	// A candidate cut by attempt-timeout is not dead: a slow handshake (a lost
	// SYN) still means the node works. It is demoted for this long instead of
	// the full failed-cooldown and rechecked in the background.
	cascadeTimeoutCooldown = 3 * time.Second
	// Repeated cuts inside cascadeTimeoutEscalateWindow escalate the candidate
	// to the full failed-cooldown, so a node that is always too slow stops
	// costing one cut per connection.
	cascadeTimeoutEscalateAfter  = 3
	cascadeTimeoutEscalateWindow = time.Minute
	// cascadeRecheckTimeout bounds the background probe of a cut candidate and
	// cascadeRecheckInterval throttles those probes per candidate.
	cascadeRecheckTimeout  = 2 * time.Second
	cascadeRecheckInterval = 5 * time.Second
	// cascadeNestedDepthLimit is how deep selectors are followed when deciding
	// whether a candidate runs its own attempts.
	cascadeNestedDepthLimit = 3
	// cascadeMinGroupProbeTimeout is the least time granted to a candidate that
	// answers as a group: its own check probes every member, so the tighter
	// cascade `timeout` would cut members the group itself still accepts.
	cascadeMinGroupProbeTimeout = 5 * time.Second
	// defaultCascadeSwitchGrace is how long a candidate that owns the failover
	// between its own members (a pool or a nested cascade) keeps its place after
	// a failed dial. The candidate switches to another member by itself, so one
	// failed dial must not switch the whole group; the group moves on only when
	// the candidate reports that nothing is left or the window expires.
	defaultCascadeSwitchGrace = 5 * time.Second
	// cascadeGraceRetries is how many extra dials such a candidate gets inside
	// one connection. It is small on purpose: the tunnel retries the whole dial
	// as well, and that retry enters the group again with the same priority.
	cascadeGraceRetries = 2
	// cascadeCandidateReserve is the least budget kept for every candidate that
	// still follows the current one, so a single hanging dial cannot spend the
	// whole caller budget and leave the rest of the order untried. It only
	// applies to a candidate that cannot switch between its own members: such a
	// candidate has nothing to fall back to, so its dial is a plain dial.
	cascadeCandidateReserve = 1500 * time.Millisecond
	// cascadeMaxFailedCooldown caps the backoff of a candidate that keeps
	// failing dials: a node that answers a probe but cannot carry traffic must
	// stop returning every failed-cooldown and costing a broken connection per
	// cycle.
	cascadeMaxFailedCooldown = 5 * time.Minute
)

// errNoAliveMember reports a group candidate whose own check found nothing.
var errNoAliveMember = errors.New("no alive member")

// groupProber is the health check every group adapter runs over its members.
// It is all a cascade needs from a candidate that is a group itself.
type groupProber interface {
	URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error)
}

// poolChecker is the cheap check a parent asks of a candidate that keeps its own
// member list: it measures the members the candidate holds instead of sweeping
// every proxy of its providers.
type poolChecker interface {
	PoolCheck(ctx context.Context) (map[string]uint16, error)
}

// usableMemberSource is a candidate that owns the failover between its own
// members and can say how many of them are left to switch to.
type usableMemberSource interface {
	// UsableMembers is how many members the candidate can still switch to. A
	// candidate with a single one has no failover of its own, so keeping its
	// place after a failed dial would only repeat the same dial.
	UsableMembers() int
}

// The dial order is rebuilt from these ranks: the pinned/active head first,
// then warm candidates in the configured order, then the cooling ones.
const (
	cascadeRankHead uint8 = iota
	cascadeRankAlive
	cascadeRankWarm
	cascadeRankCooling
)

type CascadeOption struct {
	// Lookahead is how many live standby candidates after the active one are kept warm.
	Lookahead *int `group:"lookahead,omitempty"`
	// MaxAttempts is how many candidates a single connection may try.
	MaxAttempts *int `group:"max-attempts,omitempty"`
	// FailedCooldown is how long a failed candidate is demoted, in seconds.
	FailedCooldown *int `group:"failed-cooldown,omitempty"`
	// DialTimeout and DialRetryLimit are no longer used: the tunnel applies its
	// own budget to every group. They are kept only to warn about profiles that
	// still set them.
	DialTimeout    *int `group:"dial-timeout,omitempty"`
	DialRetryLimit *int `group:"dial-retry-limit,omitempty"`
	// PreferFirst returns to the first alive candidate after an outage instead
	// of staying on the fallback that took over. It is on by default: cascade
	// orders its candidates by priority, so a recovered priority candidate must
	// not lose to a standby that merely took over earlier.
	PreferFirst *bool `group:"prefer-first,omitempty"`
	// AttemptTimeout bounds one candidate dial in milliseconds. A candidate
	// that runs its own attempts (a nested pool or cascade) is left alone, so
	// its own budget stays in charge.
	AttemptTimeout *int `group:"attempt-timeout,omitempty"`
	// SwitchGrace is how long a candidate that runs its own attempts (a nested
	// pool or cascade) keeps its place after a failed dial, in seconds. The
	// pool switches between its members by itself, so the group moves to the
	// next candidate only when the pool reports that no member is left or when
	// the window expires without a successful dial. 0 demotes on the first
	// failure, like a plain proxy.
	SwitchGrace *int `group:"switch-grace,omitempty"`
}

type cascadeState struct {
	alive       bool
	delay       uint16
	checkedAt   time.Time
	failedUntil time.Time
	// softUntil is the end of the switch-grace window a failed dial opened for a
	// candidate that owns its own failover. While the window lasts the candidate
	// keeps its place and only its failed member is replaced.
	softUntil time.Time

	// timeoutAt and timeoutCount track candidates cut by attempt-timeout, so a
	// node that is always too slow escalates to the full cooldown.
	timeoutAt    time.Time
	timeoutCount int
	// dialFaults counts consecutive failed dials, so a candidate that keeps
	// answering probes but cannot carry traffic backs off instead of returning
	// to the rotation every failed-cooldown. A successful dial resets it.
	dialFaults int
	// recheckedAt and rechecking throttle and single-flight the background probe
	// that decides whether a cut candidate is alive.
	recheckedAt time.Time
	rechecking  bool
}

// Cascade is an ordered fallback that probes only the active candidate and a
// small lookahead window, keeps the configured priority order, and can retry
// the next candidate inside the same DialContext call. Handshakes that fail on
// the first write demote the candidate as well, so a connection is never stuck
// on a proxy that accepted TCP but cannot carry traffic.
type Cascade struct {
	*GroupBase
	disableUDP     bool
	testUrl        string
	expectedStatus string
	expectedRanges utils.IntRanges[uint16]
	interval       time.Duration
	lookahead      int
	maxAttempts    int
	failedCooldown time.Duration
	preferFirst    bool
	attemptTimeout time.Duration
	switchGrace    time.Duration
	lazy           bool

	mu         sync.RWMutex
	selected   string
	active     string
	lastActive string
	states     map[string]*cascadeState

	testing   atomic.Bool
	lastCheck atomic.TypedValue[time.Time]
	// recoveryPending single-flights the timer that re-checks the group when a
	// demoted candidate leaves its cooldown.
	recoveryPending atomic.Bool
}

// DialContext implements C.ProxyAdapter
func (c *Cascade) DialContext(ctx context.Context, metadata *C.Metadata) (conn C.Conn, err error) {
	c.maybeScheduleCheck()
	order := c.candidateOrder(c.GetProxies(true))
	if len(order) == 0 {
		return c.EmptyFallback().DialContext(ctx, metadata)
	}

	attempts := c.maxAttempts
	if attempts > len(order) {
		attempts = len(order)
	}

	var lastErr error
	var lastProxy C.Proxy
	for i := 0; i < attempts; i++ {
		proxy := order[i]
		lastProxy = proxy
		retries := c.graceRetries(proxy)
		for {
			dialCtx, cancel := c.attemptContext(ctx, proxy, attempts-i)
			conn, err = proxy.DialContext(dialCtx, metadata)
			// our own attempt-timeout fired, the caller still has time left
			cut := ctx.Err() == nil && dialCtx.Err() != nil
			cancel()
			if err == nil {
				conn.AppendToChains(c)
				c.onAttemptSucceeded(proxy.Name())
				if N.NeedHandshake(conn) {
					conn = callback.NewFirstWriteCallBackConn(conn, func(err error) {
						if err == nil {
							c.onDialSuccess()
						} else {
							c.onAttemptFailed(proxy, err)
						}
					})
				}
				return conn, nil
			}

			lastErr = err
			// The caller gave up: the dial was cut by its budget, so it says
			// nothing about the candidate and nothing must be demoted.
			if ctx.Err() != nil {
				break
			}
			if cut {
				c.onAttemptTimedOut(proxy)
				break
			}
			if !c.onAttemptFailed(proxy, err) {
				break
			}
			// The candidate owns its failover and still has a working member:
			// it has already moved to another one, so the connection stays on
			// the priority path instead of switching to the next group.
			if retries <= 0 {
				break
			}
			retries--
		}
		// The caller's deadline covers every attempt; once it is gone there is
		// no point in spending more candidate dials on this connection.
		if ctx.Err() != nil {
			break
		}
	}

	if ctx.Err() == nil && lastErr != nil {
		c.onDialFailed(lastProxy.Type(), lastErr, c.healthCheck)
	}
	return nil, lastErr
}

// attemptContext bounds one candidate dial. A candidate that switches between
// its own members (a pool or a nested cascade with more than one usable member)
// is left alone: its own budget already covers the retries it wants to make.
// Every other candidate is a plain dial that may take only its share of the
// caller's budget, so one hanging dial cannot spend the whole of it and leave
// the rest of the order untried.
func (c *Cascade) attemptContext(ctx context.Context, proxy C.Proxy, left int) (context.Context, context.CancelFunc) {
	budget := c.candidateBudget(ctx, proxy, left)
	if budget <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, budget)
}

// candidateBudget is how long one candidate dial may take. A candidate with
// more than one usable member switches between them by itself, so its own
// attempt-timeout stays in charge and the group does not bound it at all. Every
// other candidate is a plain dial that must leave the candidates after it a
// reserve of their own inside the caller's budget.
func (c *Cascade) candidateBudget(ctx context.Context, proxy C.Proxy, left int) time.Duration {
	if verdict, ok := usableMemberOf(proxy); ok && verdict.UsableMembers() > 1 {
		return 0
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return c.attemptTimeout
	}
	if left < 1 {
		left = 1
	}
	if left == 1 && c.attemptTimeout <= 0 {
		// the last candidate may spend whatever the caller has left
		return 0
	}

	remaining := time.Until(deadline)
	share := remaining - time.Duration(left-1)*cascadeCandidateReserve
	if share <= 0 {
		// the caller's budget is shorter than the reserve of the rest: split
		// what is left instead of letting this candidate take all of it
		share = remaining / time.Duration(left)
	}
	if share <= 0 {
		return c.attemptTimeout
	}
	if c.attemptTimeout > 0 && share > c.attemptTimeout {
		return c.attemptTimeout
	}
	return share
}

// runsOwnAttempts reports whether the candidate retries its members itself:
// a nested pool or cascade, also when reached through selectors. Bounding such
// a candidate here would cut the attempts it is designed to make.
func runsOwnAttempts(proxy C.Proxy) bool {
	for depth := 0; depth <= cascadeNestedDepthLimit; depth++ {
		switch adapter := proxy.Adapter().(type) {
		case *URLTestPool, *Cascade:
			return true
		case *Selector:
			proxy = adapter.selectedProxy(false)
		default:
			return false
		}
	}
	return false
}

// usableMemberOf returns the verdict of a candidate that owns its failover,
// following selectors the same way runsOwnAttempts does.
func usableMemberOf(proxy C.Proxy) (usableMemberSource, bool) {
	for depth := 0; depth <= cascadeNestedDepthLimit; depth++ {
		switch adapter := proxy.Adapter().(type) {
		case *URLTestPool:
			return adapter, true
		case *Cascade:
			return adapter, true
		case *Selector:
			proxy = adapter.selectedProxy(false)
		default:
			return nil, false
		}
	}
	return nil, false
}

// ListenPacketContext implements C.ProxyAdapter
func (c *Cascade) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (pc C.PacketConn, err error) {
	c.maybeScheduleCheck()
	order := c.candidateOrder(c.GetProxies(true))
	if len(order) == 0 {
		return c.EmptyFallback().ListenPacketContext(ctx, metadata)
	}

	attempts := c.maxAttempts
	if attempts > len(order) {
		attempts = len(order)
	}

	var lastErr error
	var lastProxy C.Proxy
	for i := 0; i < attempts; i++ {
		proxy := order[i]
		lastProxy = proxy
		retries := c.graceRetries(proxy)
		for {
			dialCtx, cancel := c.attemptContext(ctx, proxy, attempts-i)
			pc, err = proxy.ListenPacketContext(dialCtx, metadata)
			// our own attempt-timeout fired, the caller still has time left
			cut := ctx.Err() == nil && dialCtx.Err() != nil
			cancel()
			if err == nil {
				pc.AppendToChains(c)
				c.onAttemptSucceeded(proxy.Name())
				return pc, nil
			}

			lastErr = err
			// the caller gave up: the attempt was cut by its budget, so it says
			// nothing about the candidate
			if ctx.Err() != nil {
				break
			}
			if cut {
				c.onAttemptTimedOut(proxy)
				break
			}
			if !c.onAttemptFailed(proxy, err) {
				break
			}
			if retries <= 0 {
				break
			}
			retries--
		}
		if ctx.Err() != nil {
			break
		}
	}

	if ctx.Err() == nil && lastErr != nil {
		c.onDialFailed(lastProxy.Type(), lastErr, c.healthCheck)
	}
	return nil, lastErr
}

// SupportUDP implements C.ProxyAdapter
func (c *Cascade) SupportUDP() bool {
	if c.disableUDP {
		return false
	}
	return c.pick(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (c *Cascade) IsL3Protocol(metadata *C.Metadata) bool {
	return c.pick(false).IsL3Protocol(metadata)
}

// Unwrap implements C.ProxyAdapter
func (c *Cascade) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return c.pick(touch)
}

// Now implements ProxyGroup
func (c *Cascade) Now() string {
	return c.pick(false).Name()
}

// Providers implements ProxyGroup
func (c *Cascade) Providers() []P.ProxyProvider {
	return c.providers
}

// Proxies implements ProxyGroup
func (c *Cascade) Proxies() []C.Proxy {
	return c.GetProxies(false)
}

// Set implements SelectAble
func (c *Cascade) Set(name string) error {
	for _, proxy := range c.GetProxies(false) {
		if proxy.Name() == name {
			c.ForceSet(name)
			return nil
		}
	}
	return errors.New("proxy not exist")
}

// ForceSet implements SelectAble
func (c *Cascade) ForceSet(name string) {
	c.mu.Lock()
	c.selected = name
	c.mu.Unlock()
}

// URLTest implements ProxyGroup. Testing the group's own URL re-runs the
// group's own check, so a host that tests the group instead of its members lets
// a recovered priority candidate take over. Any other URL is a plain per-member
// test and only reports delays.
func (c *Cascade) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	if url != c.testUrl {
		return c.GroupBase.URLTest(ctx, url, expectedStatus)
	}
	results := c.runCheck(ctx, true)
	if len(results) == 0 {
		return results, errors.New("get delay: all proxies timeout")
	}
	return results, nil
}

// SelfTestTarget implements SelfTester: the URL and status the group checks
// itself with, so a host can run the group's own test instead of dialing it.
func (c *Cascade) SelfTestTarget() (string, utils.IntRanges[uint16]) {
	return c.testUrl, c.expectedRanges
}

// MarshalJSON implements C.ProxyAdapter
func (c *Cascade) MarshalJSON() ([]byte, error) {
	all := make([]string, 0)
	for _, proxy := range c.GetProxies(false) {
		all = append(all, proxy.Name())
	}

	c.mu.RLock()
	active := c.active
	selected := c.selected
	c.mu.RUnlock()

	// The type is the adapter's own name: hosts that know cascade show it as
	// such, and the built-in fields below still let the rest render the ordered
	// candidates and the selected one.
	return json.Marshal(map[string]any{
		"type":           C.Cascade.String(),
		"now":            c.Now(),
		"all":            all,
		"active":         active,
		"lookahead":      c.lookahead,
		"preferFirst":    c.preferFirst,
		"attemptTimeout": c.attemptTimeout.Milliseconds(),
		"switchGrace":    c.switchGrace.Milliseconds(),
		"testUrl":        c.testUrl,
		"expectedStatus": c.expectedStatus,
		"fixed":          selected,
		"hidden":         c.Hidden(),
		"icon":           c.Icon(),
		"emptyFallback":  c.EmptyFallback().Name(),
	})
}

func (c *Cascade) pick(touch bool) C.Proxy {
	if touch || !c.lazy {
		c.maybeScheduleCheck()
	}

	proxies := c.GetProxies(touch)
	if len(proxies) == 0 {
		return c.EmptyFallback()
	}

	order := c.candidateOrder(proxies)
	if len(order) == 0 {
		return c.EmptyFallback()
	}
	return order[0]
}

// AliveMember reports whether the group still holds a candidate that can carry
// traffic, so a cascade that nests this one can keep it in charge while any of
// its candidates works.
func (c *Cascade) AliveMember() (alive, known bool) {
	proxies := c.GetProxies(false)
	if len(proxies) == 0 {
		return false, true
	}
	if _, ok := c.lastCheck.LoadOk(); !ok {
		return false, false
	}

	now := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, proxy := range proxies {
		if state, ok := c.states[proxy.Name()]; ok && state.alive && !state.failedUntil.After(now) {
			return true, true
		}
	}
	return false, true
}

// UsableMembers counts the candidates a parent could switch to right now: the
// ones that are not inside their failed cooldown. A cascade with a single one
// has no failover of its own, so its parent must not keep its place after a
// failed dial.
func (c *Cascade) UsableMembers() int {
	proxies := c.GetProxies(false)
	now := time.Now()

	c.mu.RLock()
	defer c.mu.RUnlock()

	usable := 0
	seen := make(map[string]struct{}, len(proxies))
	for _, proxy := range proxies {
		name := proxy.Name()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if state, ok := c.states[name]; ok && state.failedUntil.After(now) {
			continue
		}
		usable++
	}
	return usable
}

// candidateOrder returns the dialing priority: the pinned candidate, the last
// known good one, warm alive candidates, then everything else in the configured
// order. When every candidate is inside its failed cooldown (a full outage), the
// last known good one is promoted right after the pinned candidate, so recovery
// starts from the path that worked before the outage. The rest of the failed
// ones stay at the end so a short outage does not lock them out forever.
//
// The order is built on every dial and on every API poll of the group, so the
// states are read in place under the lock and the result is assembled from
// per-candidate ranks instead of copied lookup maps.
func (c *Cascade) candidateOrder(proxies []C.Proxy) []C.Proxy {
	if len(proxies) == 0 {
		return nil
	}

	now := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()

	failed := func(name string) bool {
		state, ok := c.states[name]
		return ok && state.failedUntil.After(now)
	}

	allFailed := true
	for _, proxy := range proxies {
		if !failed(proxy.Name()) {
			allFailed = false
			break
		}
	}

	// Duplicate names can come from overlapping providers; every candidate must
	// stay in the order once, otherwise a failed proxy could be tried twice in
	// the same connection.
	order := make([]C.Proxy, 0, len(proxies))
	seen := make(map[string]struct{}, len(proxies))
	add := func(proxy C.Proxy) {
		name := proxy.Name()
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		order = append(order, proxy)
	}

	// The head is the pinned candidate and, unless prefer-first is enabled,
	// the active ones: a handful of names at most, so a short scan is cheaper
	// than a separate pass over the list. With prefer-first the active name is
	// not pinned, so a recovered higher-priority candidate becomes first again
	// in the normal rank walk below.
	addHead := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		for _, proxy := range proxies {
			if proxy.Name() == name {
				add(proxy)
				return
			}
		}
	}

	if !failed(c.selected) {
		addHead(c.selected)
	}
	if allFailed {
		addHead(c.lastActive)
	}
	if !c.preferFirst {
		if !failed(c.active) {
			addHead(c.active)
		}
		if !failed(c.lastActive) {
			addHead(c.lastActive)
		}
	}

	ranks := make([]uint8, len(proxies))
	for i, proxy := range proxies {
		name := proxy.Name()
		if _, ok := seen[name]; ok {
			ranks[i] = cascadeRankHead
			continue
		}
		state, ok := c.states[name]
		switch {
		case ok && state.alive && !state.failedUntil.After(now):
			ranks[i] = cascadeRankAlive
		case !ok || !state.failedUntil.After(now):
			ranks[i] = cascadeRankWarm
		default:
			ranks[i] = cascadeRankCooling
		}
	}

	for rank := cascadeRankAlive; rank <= cascadeRankCooling; rank++ {
		for i, proxy := range proxies {
			if ranks[i] == rank {
				add(proxy)
			}
		}
	}

	return order
}

func (c *Cascade) maybeScheduleCheck() {
	if last, ok := c.lastCheck.LoadOk(); ok && time.Since(last) < c.interval {
		return
	}
	c.scheduleCheck()
}

func (c *Cascade) scheduleCheck() {
	if !c.testing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.testing.Store(false)
		c.check()
	}()
}

// healthCheck is called by GroupBase when the same proxy failed several times in a row
func (c *Cascade) healthCheck() {
	if last, ok := c.lastCheck.LoadOk(); ok && time.Since(last) < c.interval {
		return
	}
	c.scheduleCheck()
}

// check runs the periodic probe in the background.
func (c *Cascade) check() {
	c.runCheck(context.Background(), false)
}

// runCheck probes the active candidate and then walks forward in the configured
// order until it has `lookahead+1` alive candidates or the probe budget is
// exhausted. It never sweeps the whole member list, which is what separates
// cascade from fallback.
//
// A demoted candidate ahead of the active one is probed even when the walk
// already found enough alive candidates: with lookahead 0 the first alive
// candidate ends the cycle, so without that probe the group would stay on the
// fallback and the priority path would never come back. With force set the
// candidate's cooldown is ignored, which is what an explicit test of the group
// asks for.
func (c *Cascade) runCheck(ctx context.Context, force bool) map[string]uint16 {
	proxies := c.GetProxies(true)
	if !c.proxiesReady(proxies) {
		return nil
	}

	order := c.candidateOrder(proxies)
	if len(order) == 0 {
		return nil
	}

	target := c.lookahead + 1
	budget := target * cascadeProbeBudgetFactor
	if budget > len(order) {
		budget = len(order)
	}
	// The walk stops after target candidates answered, so only the members it
	// can reach are queued: a later candidate is not scheduled for this check.
	endCheck := beginCheckScope(c.Name(), c.testUrl, order[:budget])
	defer endCheck()
	results := make(map[string]uint16, budget+1)
	tested := make(map[string]struct{}, budget+1)
	alive := 0
	probed := 0
	for _, proxy := range order {
		if alive >= target || probed >= budget || ctx.Err() != nil {
			break
		}

		probed++
		tested[proxy.Name()] = struct{}{}
		delay, err := c.probeCandidate(ctx, proxy)
		if err != nil {
			if ctx.Err() != nil {
				// the caller gave up: the probe was cut, so it says nothing
				// about the candidate and the walk ends here
				break
			}
			c.markUnavailable(proxy.Name())
			continue
		}
		results[proxy.Name()] = delay
		c.markAlive(proxy.Name(), delay)
		alive++
	}

	if proxy, ok := c.recoveryCandidate(proxies, force); ok && ctx.Err() == nil {
		if _, alreadyTested := tested[proxy.Name()]; !alreadyTested {
			probed++
			delay, err := c.probeCandidate(ctx, proxy)
			if err != nil {
				if ctx.Err() == nil {
					c.markUnavailable(proxy.Name())
				}
			} else {
				results[proxy.Name()] = delay
				c.markAlive(proxy.Name(), delay)
				alive++
			}
		}
	}

	c.settleActive(proxies)
	c.pruneStates(proxies)
	c.lastCheck.Store(time.Now())
	c.rescheduleRecovery(proxies)
	log.Debugln("ProxyGroup: %s cascade check probed %d candidates with %d alive", c.Name(), probed, alive)
	return results
}

// probeCandidate measures one candidate for the group's own test URL. A
// candidate that answers as a group (a pool, a load-balance or a nested
// cascade) is asked through its own check: a single dial through it exercises
// one member only, so one unlucky member must not demote a candidate whose
// other members still work.
func (c *Cascade) probeCandidate(ctx context.Context, proxy C.Proxy) (uint16, error) {
	probeCtx, cancel := context.WithTimeout(ctx, c.probeTimeout(proxy))
	defer cancel()

	group, ok := probeGroup(proxy)
	if !ok {
		return probequeue.TestProxy(probeCtx, proxy, c.testUrl, c.expectedRanges)
	}

	// A candidate that keeps its own member list is asked for its own view: its
	// quick check measures the members it holds, while a group-wide check would
	// sweep every candidate of its providers and rebuild it from that partial
	// result.
	if checker, ok := group.(poolChecker); ok {
		results, err := checker.PoolCheck(probeCtx)
		if err != nil {
			return 0, err
		}
		return shortestDelay(results)
	}

	url, expected := c.testUrl, c.expectedRanges
	if tester, ok := group.(SelfTester); ok {
		url, expected = tester.SelfTestTarget()
	}
	results, err := group.URLTest(probeCtx, url, expected)
	if err != nil {
		return 0, err
	}
	return shortestDelay(results)
}

// probeGroup reports whether the candidate answers as a group with its own
// health check, following selectors the same way runsOwnAttempts does.
func probeGroup(proxy C.Proxy) (groupProber, bool) {
	for depth := 0; depth <= cascadeNestedDepthLimit; depth++ {
		switch adapter := proxy.Adapter().(type) {
		case *URLTestPool:
			return adapter, true
		case *Cascade:
			return adapter, true
		case *LoadBalance:
			return adapter, true
		case *Selector:
			proxy = adapter.selectedProxy(false)
		default:
			return nil, false
		}
	}
	return nil, false
}

// probeTimeout bounds one candidate probe. A candidate that answers as a group
// gets at least cascadeMinGroupProbeTimeout, so the cascade `timeout` does not
// cut the members its own check still accepts.
func (c *Cascade) probeTimeout(proxy C.Proxy) time.Duration {
	timeout := time.Duration(c.testTimeout) * time.Millisecond
	if _, ok := probeGroup(proxy); ok && timeout < cascadeMinGroupProbeTimeout {
		timeout = cascadeMinGroupProbeTimeout
	}
	return timeout
}

// shortestDelay returns the best delay of a group check, so a candidate that
// answers through any of its members is ranked by its fastest one.
func shortestDelay(results map[string]uint16) (uint16, error) {
	best := uint16(0xffff)
	for _, delay := range results {
		if delay < best {
			best = delay
		}
	}
	if best == 0xffff {
		return 0, errNoAliveMember
	}
	return best, nil
}

// recoveryCandidate returns the highest-priority demoted candidate that ranks
// above the active one. The check walk never reaches it once lookahead is
// satisfied, so it is probed separately; with force set its cooldown is
// ignored, which is what an explicit test of the group asks for.
func (c *Cascade) recoveryCandidate(proxies []C.Proxy, force bool) (C.Proxy, bool) {
	now := time.Now()

	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, proxy := range proxies {
		if proxy.Name() == c.active {
			break
		}
		state, ok := c.states[proxy.Name()]
		if !ok || state.alive {
			continue
		}
		if !force && state.failedUntil.After(now) {
			continue
		}
		return proxy, true
	}
	return nil, false
}

// scheduleRecovery re-checks the group once the cooldown ends, so a demoted
// priority candidate returns to the rotation by itself instead of waiting a
// whole interval. Only one timer is armed at a time; the next check re-arms it
// for whatever is still cooling.
func (c *Cascade) scheduleRecovery(after time.Duration) {
	if after <= 0 || !c.recoveryPending.CompareAndSwap(false, true) {
		return
	}
	time.AfterFunc(after, func() {
		c.recoveryPending.Store(false)
		c.scheduleCheck()
	})
}

// rescheduleRecovery arms the timer for the nearest cooldown expiry among the
// candidates that rank above the active one.
func (c *Cascade) rescheduleRecovery(proxies []C.Proxy) {
	now := time.Now()

	c.mu.RLock()
	soonest := time.Duration(0)
	for _, proxy := range proxies {
		if proxy.Name() == c.active {
			break
		}
		state, ok := c.states[proxy.Name()]
		if !ok || state.alive || !state.failedUntil.After(now) {
			continue
		}
		if wait := state.failedUntil.Sub(now); soonest == 0 || wait < soonest {
			soonest = wait
		}
	}
	c.mu.RUnlock()

	c.scheduleRecovery(soonest)
}

func (c *Cascade) onAttemptSucceeded(name string) {
	c.mu.Lock()
	state := c.ensureStateLocked(name)
	state.alive = true
	state.failedUntil = time.Time{}
	state.softUntil = time.Time{}
	state.dialFaults = 0
	state.checkedAt = time.Now()
	c.active = name
	c.lastActive = name
	c.mu.Unlock()
}

// onAttemptFailed records a failed dial and reports whether the candidate kept
// its place. A candidate that owns the failover between its own members is not
// demoted while it still has a working member inside the switch-grace window:
// the pool moves to another member and the next attempt through it is expected
// to succeed.
func (c *Cascade) onAttemptFailed(proxy C.Proxy, err error) bool {
	if !shouldTrackDialFailure(proxy.Type(), err) {
		return false
	}
	if c.softFailure(proxy) {
		return true
	}
	c.markDialUnavailable(proxy.Name())
	return false
}

// softFailure reports whether a failed dial must not demote the candidate. The
// first failure opens the switch-grace window; the failures inside it keep the
// candidate, and the first one after it demotes it so the group finally moves
// on. A candidate with nothing left to switch to is cut at once: the window
// would only repeat the dial that just failed.
func (c *Cascade) softFailure(proxy C.Proxy) bool {
	if c.switchGrace <= 0 || !runsOwnAttempts(proxy) {
		return false
	}

	if verdict, ok := usableMemberOf(proxy); ok {
		if verdict.UsableMembers() <= 1 {
			log.Debugln("ProxyGroup: %s candidate %s has no other member to switch to", c.Name(), proxy.Name())
			return false
		}
	}

	now := time.Now()
	c.mu.Lock()
	state := c.ensureStateLocked(proxy.Name())
	opened := state.softUntil.IsZero()
	kept := opened || now.Before(state.softUntil)
	if opened {
		state.softUntil = now.Add(c.switchGrace)
	} else if !kept {
		state.softUntil = time.Time{}
	}
	c.mu.Unlock()

	if opened {
		log.Debugln("ProxyGroup: %s candidate %s keeps its place for %s after a failed dial", c.Name(), proxy.Name(), c.switchGrace)
	}
	return kept
}

// graceRetries is how many extra dials a candidate that owns its failover gets
// inside one connection: every failed dial through it replaces the member that
// failed, so an immediate retry stays on the priority path. A candidate with a
// single member has nothing to replace, so the retry would only repeat the same
// dial and the group moves on instead.
func (c *Cascade) graceRetries(proxy C.Proxy) int {
	if c.switchGrace <= 0 || !runsOwnAttempts(proxy) {
		return 0
	}
	if verdict, ok := usableMemberOf(proxy); ok && verdict.UsableMembers() <= 1 {
		return 0
	}
	return cascadeGraceRetries
}

// onAttemptTimedOut records a candidate that our own attempt-timeout cut. The
// node is not dead — a slow handshake still means it works — so it is demoted
// for cascadeTimeoutCooldown instead of the full failed-cooldown, and a
// background probe clears the demotion as soon as the node answers. Repeated
// cuts inside cascadeTimeoutEscalateWindow mean the node is genuinely too slow
// and escalate it to the full failed-cooldown.
func (c *Cascade) onAttemptTimedOut(proxy C.Proxy) {
	now := time.Now()

	c.mu.Lock()
	state := c.ensureStateLocked(proxy.Name())
	if !state.timeoutAt.IsZero() && now.Sub(state.timeoutAt) > cascadeTimeoutEscalateWindow {
		state.timeoutCount = 0
	}
	state.timeoutAt = now
	state.timeoutCount++
	count := state.timeoutCount
	escalated := count >= cascadeTimeoutEscalateAfter && c.failedCooldown > 0

	cooldown := cascadeTimeoutCooldown
	if escalated {
		cooldown = c.failedCooldown
	} else if c.failedCooldown > 0 && c.failedCooldown < cooldown {
		cooldown = c.failedCooldown
	}
	if cooldown > 0 {
		state.failedUntil = now.Add(cooldown)
	}
	if c.active == proxy.Name() {
		c.active = ""
	}

	recheck := !escalated && !state.rechecking && now.Sub(state.recheckedAt) >= cascadeRecheckInterval
	if recheck {
		state.recheckedAt = now
		state.rechecking = true
	}
	c.mu.Unlock()

	if escalated {
		log.Debugln("ProxyGroup: %s candidate %s was cut by attempt-timeout %d times, demoted for %s", c.Name(), proxy.Name(), count, c.failedCooldown)
	}
	if !recheck {
		return
	}
	go c.recheckCandidate(proxy)
}

// recheckCandidate probes a cut candidate in the background, so a slow but
// working node returns to the rotation without waiting for the next full check.
func (c *Cascade) recheckCandidate(proxy C.Proxy) {
	defer func() {
		c.mu.Lock()
		if state, ok := c.states[proxy.Name()]; ok {
			state.rechecking = false
		}
		c.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), c.recheckTimeout(proxy))
	delay, err := c.probeCandidate(ctx, proxy)
	cancel()
	if err != nil {
		return
	}
	c.markAlive(proxy.Name(), delay)
}

// recheckTimeout bounds the background probe of a cut candidate, keeping the
// floor a group candidate needs for its own check.
func (c *Cascade) recheckTimeout(proxy C.Proxy) time.Duration {
	timeout := c.probeTimeout(proxy)
	if timeout < cascadeRecheckTimeout {
		timeout = cascadeRecheckTimeout
	}
	return timeout
}

func (c *Cascade) markUnavailable(name string) {
	c.markUnavailableFor(name, c.failedCooldown)
}

// markDialUnavailable demotes a candidate a dial failed through. Consecutive
// failures double the cooldown up to cascadeMaxFailedCooldown: a candidate that
// answers a probe but cannot carry traffic must stop returning every
// failed-cooldown and costing a broken connection per cycle. A successful dial
// resets the count.
func (c *Cascade) markDialUnavailable(name string) {
	cooldown := c.failedCooldown

	c.mu.Lock()
	state := c.ensureStateLocked(name)
	state.dialFaults++
	for faults := state.dialFaults; faults > 1 && cooldown > 0 && cooldown < cascadeMaxFailedCooldown; faults-- {
		cooldown *= 2
	}
	c.mu.Unlock()

	if cooldown > cascadeMaxFailedCooldown {
		cooldown = cascadeMaxFailedCooldown
	}
	c.markUnavailableFor(name, cooldown)
}

func (c *Cascade) markUnavailableFor(name string, cooldown time.Duration) {
	c.mu.Lock()
	state := c.ensureStateLocked(name)
	state.alive = false
	state.softUntil = time.Time{}
	if cooldown > 0 {
		state.failedUntil = time.Now().Add(cooldown)
	} else {
		state.failedUntil = time.Time{}
	}
	if c.active == name {
		c.active = ""
	}
	c.mu.Unlock()

	c.scheduleRecovery(cooldown)
}

func (c *Cascade) markAlive(name string, delay uint16) {
	c.mu.Lock()
	state := c.ensureStateLocked(name)
	state.alive = true
	state.delay = delay
	// A probe reaches the test URL, not the traffic the connection carries: it
	// clears a demotion the check itself earned, but a candidate with failed
	// dials keeps the backoff it earned until a dial proves it again.
	if state.dialFaults == 0 || !state.failedUntil.After(time.Now()) {
		state.failedUntil = time.Time{}
	}
	state.softUntil = time.Time{}
	state.checkedAt = time.Now()
	c.mu.Unlock()
}

func (c *Cascade) ensureStateLocked(name string) *cascadeState {
	state, ok := c.states[name]
	if !ok {
		state = &cascadeState{}
		c.states[name] = state
	}
	return state
}

// settleActive moves the active candidate to the first alive one when the
// current active died. Without prefer-first a recovered higher-priority
// candidate never preempts the active one on its own: that switch happens only
// when the active fails, which keeps the group from flapping between two
// healthy candidates. With prefer-first the group always returns to the first
// alive candidate in the configured order.
func (c *Cascade) settleActive(proxies []C.Proxy) {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	alive := func(name string) bool {
		state, ok := c.states[name]
		return ok && state.alive && !state.failedUntil.After(now)
	}

	if c.selected != "" && alive(c.selected) {
		c.active = c.selected
		return
	}
	if !c.preferFirst && c.active != "" && alive(c.active) {
		return
	}

	c.active = ""
	for _, proxy := range proxies {
		if alive(proxy.Name()) {
			c.active = proxy.Name()
			return
		}
	}
}

func (c *Cascade) pruneStates(proxies []C.Proxy) {
	names := make(map[string]struct{}, len(proxies))
	for _, proxy := range proxies {
		names[proxy.Name()] = struct{}{}
	}

	c.mu.Lock()
	for name := range c.states {
		if _, ok := names[name]; !ok {
			delete(c.states, name)
		}
	}
	c.mu.Unlock()
}

// proxiesReady reports whether the providers already loaded their proxies,
// otherwise a check would probe the empty fallback.
func (c *Cascade) proxiesReady(proxies []C.Proxy) bool {
	return len(proxies) != 1 || proxies[0] != c.EmptyFallback()
}

func NewCascade(option GroupCommonOption, cascadeOption CascadeOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*Cascade, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}

	expectedStatus, err := utils.NewUnsignedRanges[uint16](option.ExpectedStatus)
	if err != nil {
		return nil, err
	}

	interval := time.Duration(option.Interval) * time.Second
	if interval <= 0 {
		interval = defaultCascadeInterval
	}

	lookahead := defaultCascadeLookahead
	if cascadeOption.Lookahead != nil {
		lookahead = *cascadeOption.Lookahead
		if lookahead < 0 {
			lookahead = 0
		}
	}

	maxAttempts := lookahead + 1
	if cascadeOption.MaxAttempts != nil {
		maxAttempts = *cascadeOption.MaxAttempts
		if maxAttempts < 1 {
			maxAttempts = 1
		}
	}

	failedCooldown := defaultCascadeFailedCooldown
	if cascadeOption.FailedCooldown != nil {
		failedCooldown = time.Duration(*cascadeOption.FailedCooldown) * time.Second
		if failedCooldown < 0 {
			failedCooldown = 0
		}
	}

	// The tunnel owns the dial budget of every group, so these keys no longer
	// change anything. A profile that still sets them is told instead of being
	// silently ignored by the decoder.
	if cascadeOption.DialTimeout != nil || cascadeOption.DialRetryLimit != nil {
		log.Warnln("ProxyGroup: %s ignores dial-timeout/dial-retry-limit, the tunnel applies its own dial budget", option.Name)
	}

	// The configured order is the priority, so the group returns to the first
	// alive candidate as soon as it answers again; prefer-first: false keeps the
	// sticky behaviour for a profile that wants a switch only on failure.
	preferFirst := true
	if cascadeOption.PreferFirst != nil {
		preferFirst = *cascadeOption.PreferFirst
	}

	var attemptTimeout time.Duration
	if cascadeOption.AttemptTimeout != nil {
		attemptTimeout = time.Duration(*cascadeOption.AttemptTimeout) * time.Millisecond
		if attemptTimeout < 0 {
			attemptTimeout = 0
		}
	}

	switchGrace := defaultCascadeSwitchGrace
	if cascadeOption.SwitchGrace != nil {
		switchGrace = time.Duration(*cascadeOption.SwitchGrace) * time.Second
		if switchGrace < 0 {
			switchGrace = 0
		}
	}

	testUrl := option.URL
	if testUrl == "" {
		testUrl = C.DefaultTestURL
	}

	return &Cascade{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Cascade,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		disableUDP:     option.DisableUDP,
		testUrl:        testUrl,
		expectedStatus: option.ExpectedStatus,
		expectedRanges: expectedStatus,
		interval:       interval,
		lookahead:      lookahead,
		maxAttempts:    maxAttempts,
		failedCooldown: failedCooldown,
		preferFirst:    preferFirst,
		attemptTimeout: attemptTimeout,
		switchGrace:    switchGrace,
		lazy:           option.Lazy,
		states:         make(map[string]*cascadeState),
	}, nil
}
