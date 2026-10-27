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
)

type URLTestPoolOption struct {
	PoolSize       int    `group:"pool-size,omitempty"`
	FullInterval   int    `group:"full-interval,omitempty"`
	DegradedChecks *int   `group:"degraded-checks,omitempty"`
	Tolerance      uint16 `group:"tolerance,omitempty"`
}

// URLTestPool keeps a fixed pool of the fastest proxies.
// A quick health check tests only the pool members every `interval`,
// a full health check tests all proxies every `full-interval` and rebuilds
// the pool. When the pool stays underfilled (some members are dead) for
// several quick checks in a row, a full check is started early to refill it.
// Dial failures mark the used proxy as suspicious so the next connection is
// routed to the next best pool member.
type URLTestPool struct {
	*GroupBase
	disableUDP     bool
	testUrl        string
	expectedStatus string
	expectedRanges utils.IntRanges[uint16]
	interval       time.Duration
	fullInterval   time.Duration
	poolSize       int
	degradedChecks int
	tolerance      uint16
	lazy           bool

	mu            sync.RWMutex
	selected      string
	pool          []string
	lastPool      []string
	lastGood      string
	suspicious    map[string]struct{}
	fastNode      C.Proxy
	lastEmptyFull time.Time
	degradedCount int

	testing      atomic.Bool
	recovering   atomic.Bool
	lastQuick    atomic.TypedValue[time.Time]
	lastFull     atomic.TypedValue[time.Time]
	lastRecovery atomic.TypedValue[time.Time]
	hasFullCheck atomic.Bool
}

// DialContext implements C.ProxyAdapter
func (u *URLTestPool) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	proxy, recovery := u.fastForDial(true)
	c, err = proxy.DialContext(ctx, metadata)
	if err != nil {
		u.onProxyFailed(proxy, err)
		if recovery {
			if fallback := u.recoverAfterFailure(ctx); fallback != nil {
				proxy = fallback
				c, err = proxy.DialContext(ctx, metadata)
				if err != nil {
					u.onProxyFailed(proxy, err)
				}
			}
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

// ListenPacketContext implements C.ProxyAdapter
func (u *URLTestPool) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy, _ := u.fastForDial(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(u)
		u.noteDialSucceeded(proxy)
	} else {
		u.onProxyFailed(proxy, err)
	}
	return pc, err
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

	// Hosts pick a group's UI by its type name, and URLTestPool is not one they
	// know; report the closest built-in type so the selected node stays visible,
	// while the real type remains on the adapter itself.
	return json.Marshal(map[string]any{
		"type":           C.URLTest.String(),
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
	if len(u.pool) == 0 {
		return nil
	}

	lookup := make(map[string]C.Proxy, len(proxies))
	for _, proxy := range proxies {
		lookup[proxy.Name()] = proxy
	}

	pool := make([]C.Proxy, 0, len(u.pool))
	for _, name := range u.pool {
		if proxy, ok := lookup[name]; ok {
			pool = append(pool, proxy)
		}
	}
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
// It is throttled by `interval` after a full check that found nothing alive.
func (u *URLTestPool) requestFullCheckOnExhaustion() {
	u.mu.RLock()
	lastEmpty := u.lastEmptyFull
	u.mu.RUnlock()

	if !lastEmpty.IsZero() && time.Since(lastEmpty) < u.interval {
		return
	}
	u.scheduleCheck(true)
}

func (u *URLTestPool) scheduleCheck(full bool) {
	if !u.testing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer u.testing.Store(false)
		if full {
			u.fullCheck()
		} else {
			u.quickCheck()
		}
	}()
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
	lookup := make(map[string]C.Proxy, len(proxies))
	for _, proxy := range proxies {
		lookup[proxy.Name()] = proxy
	}

	poolProxies := make([]C.Proxy, 0, len(names))
	for _, name := range names {
		if proxy, ok := lookup[name]; ok {
			poolProxies = append(poolProxies, proxy)
		}
	}
	if len(poolProxies) != len(names) {
		// the proxy list has been changed, rebuild the pool
		u.fullCheck()
		return
	}

	log.Debugln("ProxyGroup: %s start quick health check for %d proxies", u.Name(), len(poolProxies))
	results := u.testProxies(poolProxies)
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

	if u.shouldRunFullCheck(len(results)) {
		log.Debugln("ProxyGroup: %s pool is underfilled for %d quick checks, start full health check", u.Name(), u.degradedChecks)
		u.fullCheck()
	}
}

// shouldRunFullCheck counts consecutive quick checks with an underfilled pool
// and reports whether a full check should rebuild the pool. The pool is
// compared with its current length, so a pool smaller than pool-size (not
// enough proxies configured) doesn't trigger pointless full checks.
func (u *URLTestPool) shouldRunFullCheck(alive int) bool {
	if u.degradedChecks <= 0 {
		return false
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	if alive >= len(u.pool) {
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
	lookup := make(map[string]C.Proxy, len(proxies))
	for _, proxy := range proxies {
		lookup[proxy.Name()] = proxy
	}
	candidates := make([]C.Proxy, 0, len(names))
	for _, name := range names {
		if proxy, ok := lookup[name]; ok {
			candidates = append(candidates, proxy)
		}
	}
	if len(candidates) == 0 {
		u.requestFullCheckOnExhaustion()
		return nil
	}

	u.lastRecovery.Store(time.Now())
	results := u.testProxiesWithTimeout(candidates, timeout)
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

	if proxy, ok := lookup[best]; ok {
		return proxy
	}
	return nil
}

func (u *URLTestPool) fullCheck() {
	proxies := u.GetProxies(true)
	if !u.proxiesReady(proxies) {
		return
	}

	log.Debugln("ProxyGroup: %s start full health check for %d proxies", u.Name(), len(proxies))
	u.applyResults(u.testProxies(proxies))
}

func (u *URLTestPool) applyResults(results map[string]uint16) {
	names := sortedProxyNames(results)
	if len(names) > u.poolSize {
		names = names[:u.poolSize]
	}

	now := time.Now()
	u.mu.Lock()
	u.pool = names
	if len(names) > 0 {
		u.lastPool = append([]string(nil), names...)
	}
	u.suspicious = make(map[string]struct{})
	u.fastNode = nil
	u.degradedCount = 0
	if len(names) == 0 {
		u.lastEmptyFull = now
	} else {
		u.lastEmptyFull = time.Time{}
	}
	u.mu.Unlock()

	u.lastFull.Store(now)
	u.lastQuick.Store(now)
	u.hasFullCheck.Store(true)
}

// testProxies tests proxies concurrently and returns delay (ms) of alive proxies
func (u *URLTestPool) testProxies(proxies []C.Proxy) map[string]uint16 {
	return u.testProxiesWithTimeout(proxies, time.Duration(u.testTimeout)*time.Millisecond)
}

// testProxiesWithTimeout is testProxies with a custom timeout, used by the
// emergency recovery check to stay inside the connection deadline.
func (u *URLTestPool) testProxiesWithTimeout(proxies []C.Proxy, timeout time.Duration) map[string]uint16 {
	results := make(map[string]uint16, len(proxies))
	var mu sync.Mutex

	group := new(errgroup.Group)
	group.SetLimit(urlTestPoolCheckLimit)
	for _, proxy := range proxies {
		proxy := proxy
		group.Go(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			delay, err := proxy.URLTest(ctx, u.testUrl, u.expectedRanges)
			if err != nil {
				return nil
			}

			mu.Lock()
			results[proxy.Name()] = delay
			mu.Unlock()
			return nil
		})
	}
	_ = group.Wait()

	return results
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

func shouldTrackDialFailure(adapterType C.AdapterType, err error) bool {
	switch adapterType {
	case C.Direct, C.Compatible, C.Reject, C.Pass, C.RejectDrop:
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

	testUrl := option.URL
	if testUrl == "" {
		testUrl = C.DefaultTestURL
	}

	return &URLTestPool{
		GroupBase: NewGroupBase(GroupBaseOption{
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
		}),
		disableUDP:     option.DisableUDP,
		testUrl:        testUrl,
		expectedStatus: option.ExpectedStatus,
		expectedRanges: expectedStatus,
		interval:       interval,
		fullInterval:   fullInterval,
		poolSize:       poolSize,
		degradedChecks: degradedChecks,
		tolerance:      poolOption.Tolerance,
		lazy:           option.Lazy,
		suspicious:     make(map[string]struct{}),
	}, nil
}
