package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

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
)

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
}

type cascadeState struct {
	alive       bool
	delay       uint16
	checkedAt   time.Time
	failedUntil time.Time

	// timeoutAt and timeoutCount track candidates cut by attempt-timeout, so a
	// node that is always too slow escalates to the full cooldown.
	timeoutAt    time.Time
	timeoutCount int
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
	lazy           bool

	mu         sync.RWMutex
	selected   string
	active     string
	lastActive string
	states     map[string]*cascadeState

	testing   atomic.Bool
	lastCheck atomic.TypedValue[time.Time]
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
		dialCtx, cancel := c.attemptContext(ctx, proxy)
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
		if cut {
			c.onAttemptTimedOut(proxy)
		} else {
			c.onAttemptFailed(proxy, err)
		}
		// The caller's deadline covers every attempt; once it is gone there is
		// no point in spending more candidate dials on this connection.
		if ctx.Err() != nil {
			break
		}
	}

	c.onDialFailed(lastProxy.Type(), lastErr, c.healthCheck)
	return nil, lastErr
}

// attemptContext bounds one candidate dial when attempt-timeout is set. A
// candidate that runs its own attempts (a nested pool or cascade) is not cut:
// its own budget already covers the retries it wants to make.
func (c *Cascade) attemptContext(ctx context.Context, proxy C.Proxy) (context.Context, context.CancelFunc) {
	if c.attemptTimeout <= 0 || runsOwnAttempts(proxy) {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.attemptTimeout)
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
		dialCtx, cancel := c.attemptContext(ctx, proxy)
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
		if cut {
			c.onAttemptTimedOut(proxy)
		} else {
			c.onAttemptFailed(proxy, err)
		}
		if ctx.Err() != nil {
			break
		}
	}

	c.onDialFailed(lastProxy.Type(), lastErr, c.healthCheck)
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

// URLTest implements ProxyGroup
func (c *Cascade) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	mp, err := c.GroupBase.URLTest(ctx, url, expectedStatus)
	if err == nil && url == c.testUrl {
		c.applyResults(mp)
	}
	return mp, err
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

// check probes the active candidate and then walks forward in the configured
// order until it has `lookahead+1` alive candidates or the probe budget is
// exhausted. It never sweeps the whole member list, which is what separates
// cascade from fallback.
func (c *Cascade) check() {
	proxies := c.GetProxies(true)
	if !c.proxiesReady(proxies) {
		return
	}

	order := c.candidateOrder(proxies)
	if len(order) == 0 {
		return
	}

	target := c.lookahead + 1
	budget := target * cascadeProbeBudgetFactor
	alive := 0
	probed := 0
	for _, proxy := range order {
		if alive >= target || probed >= budget {
			break
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.testTimeout)*time.Millisecond)
		delay, err := proxy.URLTest(ctx, c.testUrl, c.expectedRanges)
		cancel()
		probed++

		if err != nil {
			c.markUnavailable(proxy.Name())
			continue
		}
		c.markAlive(proxy.Name(), delay)
		alive++
	}

	c.settleActive(proxies)
	c.pruneStates(proxies)
	c.lastCheck.Store(time.Now())
	log.Debugln("ProxyGroup: %s cascade check probed %d candidates with %d alive", c.Name(), probed, alive)
}

func (c *Cascade) onAttemptSucceeded(name string) {
	c.mu.Lock()
	state := c.ensureStateLocked(name)
	state.alive = true
	state.failedUntil = time.Time{}
	state.checkedAt = time.Now()
	c.active = name
	c.lastActive = name
	c.mu.Unlock()
}

func (c *Cascade) onAttemptFailed(proxy C.Proxy, err error) {
	if !shouldTrackDialFailure(proxy.Type(), err) {
		return
	}
	c.markUnavailable(proxy.Name())
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

	ctx, cancel := context.WithTimeout(context.Background(), cascadeRecheckTimeout)
	delay, err := proxy.URLTest(ctx, c.testUrl, c.expectedRanges)
	cancel()
	if err != nil {
		return
	}
	c.markAlive(proxy.Name(), delay)
}

func (c *Cascade) markUnavailable(name string) {
	c.mu.Lock()
	state := c.ensureStateLocked(name)
	state.alive = false
	if c.failedCooldown > 0 {
		state.failedUntil = time.Now().Add(c.failedCooldown)
	}
	if c.active == name {
		c.active = ""
	}
	c.mu.Unlock()
}

func (c *Cascade) markAlive(name string, delay uint16) {
	c.mu.Lock()
	state := c.ensureStateLocked(name)
	state.alive = true
	state.delay = delay
	state.failedUntil = time.Time{}
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

func (c *Cascade) applyResults(results map[string]uint16) {
	proxies := c.GetProxies(false)
	now := time.Now()

	c.mu.Lock()
	for name, delay := range results {
		state := c.ensureStateLocked(name)
		state.alive = true
		state.delay = delay
		state.failedUntil = time.Time{}
		state.checkedAt = now
	}
	for _, proxy := range proxies {
		if _, ok := results[proxy.Name()]; ok {
			continue
		}
		state := c.ensureStateLocked(proxy.Name())
		state.alive = false
		if c.failedCooldown > 0 {
			state.failedUntil = now.Add(c.failedCooldown)
		}
	}
	c.mu.Unlock()

	c.settleActive(proxies)
	c.lastCheck.Store(now)
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
		lazy:           option.Lazy,
		states:         make(map[string]*cascadeState),
	}, nil
}
