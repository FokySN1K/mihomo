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
	// defaultCascadeDialTimeout and defaultCascadeDialRetryLimit are the
	// cascade's own dial budget: one connection may try several candidates,
	// and the tunnel retries the whole call with these values unless the
	// group overrides them.
	defaultCascadeDialTimeout    = C.DefaultTCPTimeout
	defaultCascadeDialRetryLimit = 10
)

type CascadeOption struct {
	// Lookahead is how many live standby candidates after the active one are kept warm.
	Lookahead *int `group:"lookahead,omitempty"`
	// MaxAttempts is how many candidates a single connection may try.
	MaxAttempts *int `group:"max-attempts,omitempty"`
	// FailedCooldown is how long a failed candidate is demoted, in seconds.
	FailedCooldown *int `group:"failed-cooldown,omitempty"`
	// DialTimeout is the budget of one connection through the cascade, in milliseconds.
	DialTimeout *int `group:"dial-timeout,omitempty"`
	// DialRetryLimit is how many times the tunnel may retry such a connection.
	DialRetryLimit *int `group:"dial-retry-limit,omitempty"`
}

type cascadeState struct {
	alive       bool
	delay       uint16
	checkedAt   time.Time
	failedUntil time.Time
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
	dialTimeout    time.Duration
	dialRetryLimit int
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
		conn, err = proxy.DialContext(ctx, metadata)
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
		c.onAttemptFailed(proxy, err)
		// The caller's deadline covers every attempt; once it is gone there is
		// no point in spending more candidate dials on this connection.
		if ctx.Err() != nil {
			break
		}
	}

	c.onDialFailed(lastProxy.Type(), lastErr, c.healthCheck)
	return nil, lastErr
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
		pc, err = proxy.ListenPacketContext(ctx, metadata)
		if err == nil {
			pc.AppendToChains(c)
			c.onAttemptSucceeded(proxy.Name())
			return pc, nil
		}

		lastErr = err
		c.onAttemptFailed(proxy, err)
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

// DialTimeout implements C.DialBudget
func (c *Cascade) DialTimeout() time.Duration {
	return c.dialTimeout
}

// DialRetryLimit implements C.DialBudget
func (c *Cascade) DialRetryLimit() int {
	return c.dialRetryLimit
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
func (c *Cascade) candidateOrder(proxies []C.Proxy) []C.Proxy {
	if len(proxies) == 0 {
		return nil
	}

	now := time.Now()
	c.mu.RLock()
	selected := c.selected
	active := c.active
	lastActive := c.lastActive
	states := make(map[string]cascadeState, len(c.states))
	for name, state := range c.states {
		states[name] = *state
	}
	c.mu.RUnlock()

	failed := func(name string) bool {
		state, ok := states[name]
		return ok && state.failedUntil.After(now)
	}
	alive := func(name string) bool {
		state, ok := states[name]
		return ok && state.alive
	}

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

	for _, proxy := range proxies {
		if proxy.Name() == selected && !failed(proxy.Name()) {
			add(proxy)
			break
		}
	}

	allFailed := true
	for _, proxy := range proxies {
		if !failed(proxy.Name()) {
			allFailed = false
			break
		}
	}
	if allFailed && lastActive != "" {
		for _, proxy := range proxies {
			if proxy.Name() == lastActive {
				add(proxy)
				break
			}
		}
	}

	for _, proxy := range proxies {
		if proxy.Name() == active && !failed(proxy.Name()) {
			add(proxy)
			break
		}
	}
	for _, proxy := range proxies {
		if proxy.Name() == lastActive && !failed(proxy.Name()) {
			add(proxy)
			break
		}
	}
	for _, proxy := range proxies {
		if alive(proxy.Name()) && !failed(proxy.Name()) {
			add(proxy)
		}
	}
	for _, proxy := range proxies {
		if !failed(proxy.Name()) {
			add(proxy)
		}
	}
	for _, proxy := range proxies {
		add(proxy)
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
// current active died. A recovered higher-priority candidate never preempts
// the active one on its own: that switch happens only when the active fails,
// which keeps the group from flapping between two healthy candidates.
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
	if c.active != "" && alive(c.active) {
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

	dialTimeout := defaultCascadeDialTimeout
	if cascadeOption.DialTimeout != nil {
		dialTimeout = time.Duration(*cascadeOption.DialTimeout) * time.Millisecond
		if dialTimeout <= 0 {
			dialTimeout = defaultCascadeDialTimeout
		}
	}

	dialRetryLimit := defaultCascadeDialRetryLimit
	if cascadeOption.DialRetryLimit != nil {
		dialRetryLimit = *cascadeOption.DialRetryLimit
		if dialRetryLimit < 1 {
			dialRetryLimit = 1
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
		dialTimeout:    dialTimeout,
		dialRetryLimit: dialRetryLimit,
		lazy:           option.Lazy,
		states:         make(map[string]*cascadeState),
	}, nil
}
