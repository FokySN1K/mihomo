package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/outboundgroup/probequeue"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type FallbackOption struct{}

type Fallback struct {
	*GroupBase
	disableUDP     bool
	testUrl        string
	selected       string
	selectedMux    sync.RWMutex
	expectedStatus string
	eventNow       string
	eventReason    string
	historyMux     sync.Mutex
}

func (f *Fallback) Now() string {
	proxy := f.findAliveProxy(false)
	return proxy.Name()
}

// DialContext implements C.ProxyAdapter
func (f *Fallback) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxy := f.findAliveProxy(true)
	c, err := proxy.DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(f)
	} else {
		f.onDialFailed(proxy.Type(), err, f.healthCheck)
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
				f.onDialSuccess()
			},
			OnDrop: func(err error) {
				f.onDialFailed(proxy.Type(), err, f.healthCheck)
			},
		})
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (f *Fallback) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := f.findAliveProxy(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(f)
	}

	return pc, err
}

// SupportUDP implements C.ProxyAdapter
func (f *Fallback) SupportUDP() bool {
	if f.disableUDP {
		return false
	}

	proxy := f.findAliveProxy(false)
	return proxy.SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (f *Fallback) IsL3Protocol(metadata *C.Metadata) bool {
	return f.findAliveProxy(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (f *Fallback) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range f.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	nowProxy, reason := f.findAliveProxyState(false)
	now := nowProxy.Name()
	fixed := f.fixedSelection()
	return json.Marshal(map[string]any{
		"type":           f.Type().String(),
		"now":            now,
		"all":            all,
		"testUrl":        f.testUrl,
		"expectedStatus": f.expectedStatus,
		"fixed":          fixed,
		"fixedActive":    fixedActive(fixed, now),
		"nowReason":      reason,
		"hidden":         f.Hidden(),
		"icon":           f.Icon(),
		"emptyFallback":  f.EmptyFallback().Name(),
	})
}

// fixedSelection returns the member the user fixed, if any.
func (f *Fallback) fixedSelection() string {
	f.selectedMux.RLock()
	defer f.selectedMux.RUnlock()
	return f.selected
}

// Unwrap implements C.ProxyAdapter
func (f *Fallback) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxy := f.findAliveProxy(touch)
	return proxy
}

func (f *Fallback) findAliveProxy(touch bool) C.Proxy {
	proxy, _ := f.findAliveProxyState(touch)
	return proxy
}

// findAliveProxyState returns the member a fallback routes through and the
// reason it was chosen. A pinned member that died is skipped for this choice,
// but the selection is not cleared: the reason reports that the pin is not
// active, and a member that comes back keeps the place the user gave it.
func (f *Fallback) findAliveProxyState(touch bool) (C.Proxy, string) {
	proxy, reason := f.chooseAliveProxy(touch)
	f.recordNow(proxy.Name(), reason)
	return proxy, reason
}

func (f *Fallback) chooseAliveProxy(touch bool) (C.Proxy, string) {
	proxies := f.GetProxies(touch)
	if len(proxies) == 0 {
		return f.EmptyFallback(), NowReasonEmpty
	}
	selected := f.fixedSelection()

	reason := NowReasonBest
	if len(selected) != 0 {
		reason = NowReasonFixedUnavailable
		for _, proxy := range proxies {
			if proxy.Name() != selected {
				continue
			}
			if proxy.AliveForTestUrl(f.testUrl) {
				return proxy, NowReasonFixed
			}
			break
		}
	}

	for _, proxy := range proxies {
		if proxy.AliveForTestUrl(f.testUrl) {
			return proxy, reason
		}
	}

	return proxies[0], NowReasonEmpty
}

// recordNow stores the routing change as a history event; a repeated read of
// the same member with the same reason is not a new event.
func (f *Fallback) recordNow(name, reason string) {
	f.historyMux.Lock()
	if f.eventNow == name && f.eventReason == reason {
		f.historyMux.Unlock()
		return
	}
	from := f.eventNow
	f.eventNow = name
	f.eventReason = reason
	f.historyMux.Unlock()

	RecordGroupEvent(GroupEvent{
		Group:  f.Name(),
		Type:   GroupEventNow,
		From:   from,
		To:     name,
		Reason: reason,
	})
}

func (f *Fallback) Set(name string) error {
	var p C.Proxy
	for _, proxy := range f.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}

	if p == nil {
		return errors.New("proxy not exist")
	}

	f.selectedMux.Lock()
	f.selected = name
	f.selectedMux.Unlock()
	if !p.AliveForTestUrl(f.testUrl) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(5000))
		defer cancel()
		expectedStatus, _ := utils.NewUnsignedRanges[uint16](f.expectedStatus)
		_, _ = probequeue.TestProxy(ctx, p, f.testUrl, expectedStatus)
	}

	return nil
}

func (f *Fallback) ForceSet(name string) {
	f.selectedMux.Lock()
	previous := f.selected
	f.selected = name
	f.selectedMux.Unlock()
	if previous != name {
		recordPinEvent(f.Name(), previous, name)
	}
}

func (f *Fallback) Providers() []P.ProxyProvider {
	return f.providers
}

func (f *Fallback) Proxies() []C.Proxy {
	return f.GetProxies(false)
}

func NewFallback(option GroupCommonOption, fallbackOption FallbackOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*Fallback, error) {
	base, err := NewGroupBase(GroupBaseOption{
		Name:           option.Name,
		Type:           C.Fallback,
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

	return &Fallback{
		GroupBase:      base,
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
	}, nil
}
