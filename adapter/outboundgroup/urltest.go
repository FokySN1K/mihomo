package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// defaultURLTestTolerance is how much faster a measured node must be than the
// current one to replace it, in milliseconds. Without the hysteresis two nodes
// with near-equal delays swap places on every check, and every swap is a new
// handshake for the next connection. url-test-pool shares this default.
const defaultURLTestTolerance uint16 = 150

type URLTestOption struct {
	// Tolerance is how much faster a measured node must be than the current one
	// to replace it, in milliseconds; 0 disables the hysteresis.
	Tolerance *uint16 `group:"tolerance,omitempty"`
}

type URLTest struct {
	*GroupBase
	selected       string
	testUrl        string
	expectedStatus string
	tolerance      uint16
	disableUDP     bool
	// stateMux guards the fixed selection and the member it resolved to: a
	// user selection can arrive while a choice is being made.
	stateMux    sync.Mutex
	fastNode    C.Proxy
	fastReason  string
	eventNow    string
	eventReason string
	fastSingle  *singledo.Single[C.Proxy]
}

func (u *URLTest) Now() string {
	return u.fast(false).Name()
}

func (u *URLTest) Set(name string) error {
	var p C.Proxy
	for _, proxy := range u.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}
	if p == nil {
		return errors.New("proxy not exist")
	}
	u.ForceSet(name)
	return nil
}

func (u *URLTest) ForceSet(name string) {
	u.stateMux.Lock()
	previous := u.selected
	u.selected = name
	u.stateMux.Unlock()
	if previous != name {
		recordPinEvent(u.Name(), previous, name)
	}
	u.fastSingle.Reset()
}

// DialContext implements C.ProxyAdapter
func (u *URLTest) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	proxy := u.fast(true)
	c, err = proxy.DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(u)
	} else {
		u.onDialFailed(proxy.Type(), err, u.healthCheck)
	}

	if N.NeedHandshake(c) && !callback.IsDataPlaneObserved(c) {
		// The dial only accepted the connection: the first byte the member
		// answers with is what proves it carries traffic. A node that takes the
		// first write and never answers is closed by the watchdog and counted
		// as a failed dial, which is what the group health check reacts to. A
		// nested group may already watch this connection: one dead flow must
		// cost one demotion, not one per wrapping level.
		c = callback.NewDataPlaneConn(c, callback.DataPlaneOptions{
			Handshake:        true,
			FirstByteTimeout: groupDataPlaneWatch,
			OnFirstByte: func(time.Duration) {
				u.onDialSuccess()
			},
			OnDrop: func(err error) {
				u.onDialFailed(proxy.Type(), err, u.healthCheck)
			},
		})
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (u *URLTest) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := u.fast(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(u)
	} else {
		u.onDialFailed(proxy.Type(), err, u.healthCheck)
	}

	return pc, err
}

// Unwrap implements C.ProxyAdapter
func (u *URLTest) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return u.fast(touch)
}

func (u *URLTest) healthCheck() {
	u.fastSingle.Reset()
	u.GroupBase.healthCheck()
}

func (u *URLTest) fast(touch bool) C.Proxy {
	elm, _, shared := u.fastSingle.Do(func() (C.Proxy, error) {
		proxies := u.GetProxies(touch)
		return u.pickFastest(proxies), nil
	})
	if shared && touch { // a shared fastSingle.Do() may cause providers untouched, so we touch them again
		u.Touch()
	}

	return elm
}

// fastState returns the member the group routes through and the reason it was
// chosen, so MarshalJSON publishes both from the same choice.
func (u *URLTest) fastState(touch bool) (C.Proxy, string) {
	proxy := u.fast(touch)
	u.stateMux.Lock()
	reason := u.fastReason
	u.stateMux.Unlock()
	if reason == "" {
		reason = NowReasonBest
	}
	return proxy, reason
}

// pickFastest applies the fixed selection and the tolerance to the current
// member list. The state it reads is guarded: a selection can arrive while a
// connection is being routed.
func (u *URLTest) pickFastest(proxies []C.Proxy) C.Proxy {
	proxy, _ := u.pickFastestState(proxies)
	return proxy
}

func (u *URLTest) pickFastestState(proxies []C.Proxy) (C.Proxy, string) {
	u.stateMux.Lock()
	proxy, reason := u.pickFastestLocked(proxies)
	u.stateMux.Unlock()
	u.recordNow(proxy.Name(), reason)
	return proxy, reason
}

func (u *URLTest) pickFastestLocked(proxies []C.Proxy) (C.Proxy, string) {
	if len(proxies) == 0 {
		u.fastNode = nil
		u.fastReason = NowReasonEmpty
		return u.EmptyFallback(), NowReasonEmpty
	}

	fixedUnavailable := false
	if u.selected != "" {
		fixedUnavailable = true
		for _, proxy := range proxies {
			if proxy.Name() != u.selected {
				continue
			}
			if proxy.AliveForTestUrl(u.testUrl) {
				u.fastNode = proxy
				u.fastReason = NowReasonFixed
				return proxy, NowReasonFixed
			}
			break
		}
	}

	fast := proxies[0]
	minDelay := fast.LastDelayForTestUrl(u.testUrl)
	fastNotExist := true

	for _, proxy := range proxies[1:] {
		if u.fastNode != nil && proxy.Name() == u.fastNode.Name() {
			fastNotExist = false
		}

		if !proxy.AliveForTestUrl(u.testUrl) {
			continue
		}

		delay := proxy.LastDelayForTestUrl(u.testUrl)
		if delay < minDelay {
			fast = proxy
			minDelay = delay
		}

	}

	// tolerance: a measured candidate replaces the current member only when
	// it is faster by more than the hysteresis band.
	switched := u.fastNode == nil || fastNotExist || !u.fastNode.AliveForTestUrl(u.testUrl) ||
		u.fastNode.LastDelayForTestUrl(u.testUrl) > fast.LastDelayForTestUrl(u.testUrl)+u.tolerance

	reason := NowReasonBest
	if fixedUnavailable {
		reason = NowReasonFixedUnavailable
	} else if !switched && fast.LastDelayForTestUrl(u.testUrl) < u.fastNode.LastDelayForTestUrl(u.testUrl) {
		reason = NowReasonTolerance
	}

	if switched {
		u.fastNode = fast
	}
	if u.fastNode == nil {
		u.fastNode = fast
	}
	u.fastReason = reason
	return u.fastNode, reason
}

// recordNow stores the routing change as a history event; a repeated read of
// the same member with the same reason is not a new event.
func (u *URLTest) recordNow(name, reason string) {
	u.stateMux.Lock()
	if u.eventNow == name && u.eventReason == reason {
		u.stateMux.Unlock()
		return
	}
	from := u.eventNow
	u.eventNow = name
	u.eventReason = reason
	u.stateMux.Unlock()

	RecordGroupEvent(GroupEvent{
		Group:  u.Name(),
		Type:   GroupEventNow,
		From:   from,
		To:     name,
		Reason: reason,
	})
}

// SupportUDP implements C.ProxyAdapter
func (u *URLTest) SupportUDP() bool {
	if u.disableUDP {
		return false
	}
	return u.fast(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (u *URLTest) IsL3Protocol(metadata *C.Metadata) bool {
	return u.fast(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (u *URLTest) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range u.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	nowProxy, reason := u.fastState(false)
	now := nowProxy.Name()
	fixed := u.fixedSelection()
	return json.Marshal(map[string]any{
		"type":           u.Type().String(),
		"now":            now,
		"all":            all,
		"testUrl":        u.testUrl,
		"expectedStatus": u.expectedStatus,
		"fixed":          fixed,
		"fixedActive":    fixedActive(fixed, now),
		"nowReason":      reason,
		"hidden":         u.Hidden(),
		"icon":           u.Icon(),
		"emptyFallback":  u.EmptyFallback().Name(),
	})
}

func (u *URLTest) Providers() []P.ProxyProvider {
	return u.providers
}

// fixedSelection returns the member the user fixed, if any.
func (u *URLTest) fixedSelection() string {
	u.stateMux.Lock()
	defer u.stateMux.Unlock()
	return u.selected
}

func (u *URLTest) Proxies() []C.Proxy {
	return u.GetProxies(false)
}

func (u *URLTest) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	return u.GroupBase.URLTest(ctx, u.testUrl, expectedStatus)
}

func NewURLTest(option GroupCommonOption, urlTestOption URLTestOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*URLTest, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}
	tolerance := defaultURLTestTolerance
	if urlTestOption.Tolerance != nil {
		tolerance = *urlTestOption.Tolerance
	}

	base, err := NewGroupBase(GroupBaseOption{
		Name:           option.Name,
		Type:           C.URLTest,
		Hidden:         option.Hidden,
		Icon:           option.Icon,
		Filter:         option.Filter,
		ExcludeFilter:  option.ExcludeFilter,
		ExcludeType:    option.ExcludeType,
		TestTimeout:    option.TestTimeout,
		MaxFailedTimes: option.MaxFailedTimes,
		EmptyFallback:  emptyFallback,
		Providers:      providers,
		UsePosition:    option.UsePosition,
	})
	if err != nil {
		return nil, err
	}

	urlTest := &URLTest{
		GroupBase:      base,
		fastSingle:     singledo.NewSingle[C.Proxy](time.Second * 10),
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
		tolerance:      tolerance,
	}

	return urlTest, nil
}
