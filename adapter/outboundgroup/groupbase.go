package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/outboundgroup/probequeue"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"

	"github.com/dlclark/regexp2"
)

// defaultGroupTestTimeout is the member budget of a group test, in
// milliseconds, when the profile does not set `timeout`.
const defaultGroupTestTimeout = 5000

// groupDataPlaneWatch bounds how long a group waits for the first byte of a
// handshake before it treats the flow as a silent node. The groups below do not
// rank their members themselves, so this only decides how soon the dead flow is
// closed and counted as a failed dial that a health check reacts to: a dial that
// accepted the route is not proof that the node carries the traffic.
const groupDataPlaneWatch = 2 * time.Second

type GroupBase struct {
	*outbound.Base
	hidden            bool
	icon              string
	filterRegs        []*regexp2.Regexp
	excludeFilterRegs []*regexp2.Regexp
	excludeTypeArray  []string
	providers         []P.ProxyProvider
	failedTestMux     sync.Mutex
	failedTimes       int
	failedTime        time.Time
	failedTesting     atomic.Bool
	testTimeout       int
	maxFailedTimes    int
	emptyFallback     C.Proxy
	usePosition       map[string]int

	// for GetProxies
	getProxiesMutex  sync.Mutex
	providerVersions []uint32
	providerProxies  []C.Proxy
	// listGen moves whenever the cached list is replaced with a new one, so a
	// reader that keeps a view of the list can tell a rebuilt answer from a
	// cached one without comparing the proxies themselves.
	listGen uint64
}

type GroupBaseOption struct {
	Name           string
	Type           C.AdapterType
	Hidden         bool
	Icon           string
	Filter         string
	ExcludeFilter  string
	ExcludeType    string
	TestTimeout    int
	MaxFailedTimes int
	EmptyFallback  C.Proxy
	Providers      []P.ProxyProvider
	UsePosition    map[string]int
}

func NewGroupBase(opt GroupBaseOption) (*GroupBase, error) {
	var excludeTypeArray []string
	if opt.ExcludeType != "" {
		excludeTypeArray = strings.Split(opt.ExcludeType, "|")
	}

	var excludeFilterRegs []*regexp2.Regexp
	if opt.ExcludeFilter != "" {
		for _, excludeFilter := range strings.Split(opt.ExcludeFilter, "`") {
			// A malformed filter is a config error the caller reports, not a
			// panic that takes the process down while the config is applied.
			excludeFilterReg, err := regexp2.Compile(excludeFilter, regexp2.None)
			if err != nil {
				return nil, fmt.Errorf("invalid exclude-filter %q: %w", excludeFilter, err)
			}
			excludeFilterRegs = append(excludeFilterRegs, excludeFilterReg)
		}
	}

	var filterRegs []*regexp2.Regexp
	if opt.Filter != "" {
		for _, filter := range strings.Split(opt.Filter, "`") {
			filterReg, err := regexp2.Compile(filter, regexp2.None)
			if err != nil {
				return nil, fmt.Errorf("invalid filter %q: %w", filter, err)
			}
			filterRegs = append(filterRegs, filterReg)
		}
	}

	gb := &GroupBase{
		Base:              outbound.NewBase(outbound.BaseOption{Name: opt.Name, Type: opt.Type}),
		hidden:            opt.Hidden,
		icon:              opt.Icon,
		filterRegs:        filterRegs,
		excludeFilterRegs: excludeFilterRegs,
		excludeTypeArray:  excludeTypeArray,
		providers:         opt.Providers,
		failedTesting:     atomic.NewBool(false),
		testTimeout:       opt.TestTimeout,
		maxFailedTimes:    opt.MaxFailedTimes,
		emptyFallback:     opt.EmptyFallback,
		usePosition:       opt.UsePosition,
	}

	if gb.testTimeout == 0 {
		gb.testTimeout = defaultGroupTestTimeout
	}
	if gb.maxFailedTimes == 0 {
		gb.maxFailedTimes = 5
	}

	return gb, nil
}

func (gb *GroupBase) Hidden() bool {
	return gb.hidden
}

func (gb *GroupBase) Icon() string {
	return gb.icon
}

func (gb *GroupBase) EmptyFallback() C.Proxy {
	return gb.emptyFallback
}

func (gb *GroupBase) Touch() {
	for _, pd := range gb.providers {
		pd.Touch()
	}
}

func (gb *GroupBase) GetProxies(touch bool) []C.Proxy {
	proxies, _ := gb.GetProxiesWithGen(touch)
	return proxies
}

// GetProxiesWithGen returns the proxies and the generation of the cached list
// they came from. The generation moves only when the list is really rebuilt,
// so a caller can reuse what it derived from the same answer.
func (gb *GroupBase) GetProxiesWithGen(touch bool) ([]C.Proxy, uint64) {
	for _, pd := range gb.providers {
		if touch { // touch first
			pd.Touch()
		}
	}

	// thread safe
	gb.getProxiesMutex.Lock()
	defer gb.getProxiesMutex.Unlock()

	// return the cached proxies if version not changed. The versions are read
	// under the lock and compared without building a slice: the dial path asks
	// for the list on every connection and must not allocate for a hit.
	if len(gb.providerVersions) == len(gb.providers) {
		changed := false
		for i, pd := range gb.providers {
			if gb.providerVersions[i] != pd.Version() {
				changed = true
				break
			}
		}
		if !changed {
			return gb.providerProxies, gb.listGen
		}
	}

	providerVersions := make([]uint32, len(gb.providers))
	for i, pd := range gb.providers {
		providerVersions[i] = pd.Version()
	}

	parts := make([][]C.Proxy, len(gb.providers))
	if len(gb.filterRegs) == 0 {
		for i, pd := range gb.providers {
			parts[i] = pd.Proxies()
		}
	} else {
		for i, pd := range gb.providers {
			if pd.VehicleType() == P.Compatible { // compatible provider unneeded filter
				parts[i] = pd.Proxies()
				continue
			}

			var newProxies []C.Proxy
			proxiesSet := map[string]struct{}{}
			for _, filterReg := range gb.filterRegs {
				for _, p := range pd.Proxies() {
					name := p.Name()
					if mat, _ := filterReg.MatchString(name); mat {
						if _, ok := proxiesSet[name]; !ok {
							proxiesSet[name] = struct{}{}
							newProxies = append(newProxies, p)
						}
					}
				}
			}
			parts[i] = newProxies
		}
	}
	proxies := gb.joinProviderProxies(parts)

	// Multiple filers means that proxies are sorted in the order in which the filers appear.
	// Although the filter has been performed once in the previous process,
	// when there are multiple providers, the array needs to be reordered as a whole.
	if len(gb.providers) > 1 && len(gb.filterRegs) > 1 {
		var newProxies []C.Proxy
		proxiesSet := map[string]struct{}{}
		for _, filterReg := range gb.filterRegs {
			for _, p := range proxies {
				name := p.Name()
				if mat, _ := filterReg.MatchString(name); mat {
					if _, ok := proxiesSet[name]; !ok {
						proxiesSet[name] = struct{}{}
						newProxies = append(newProxies, p)
					}
				}
			}
		}
		for _, p := range proxies { // add not matched proxies at the end
			name := p.Name()
			if _, ok := proxiesSet[name]; !ok {
				proxiesSet[name] = struct{}{}
				newProxies = append(newProxies, p)
			}
		}
		proxies = newProxies
	}

	if len(gb.excludeFilterRegs) > 0 {
		var newProxies []C.Proxy
	LOOP1:
		for _, p := range proxies {
			name := p.Name()
			for _, excludeFilterReg := range gb.excludeFilterRegs {
				if mat, _ := excludeFilterReg.MatchString(name); mat {
					continue LOOP1
				}
			}
			newProxies = append(newProxies, p)
		}
		proxies = newProxies
	}

	if gb.excludeTypeArray != nil {
		var newProxies []C.Proxy
	LOOP2:
		for _, p := range proxies {
			mType := p.Type().String()
			for _, excludeType := range gb.excludeTypeArray {
				if strings.EqualFold(mType, excludeType) {
					continue LOOP2
				}
			}
			newProxies = append(newProxies, p)
		}
		proxies = newProxies
	}

	if len(proxies) == 0 {
		return []C.Proxy{gb.EmptyFallback()}, gb.listGen
	}

	// only cache when proxies not empty
	gb.providerVersions = providerVersions
	gb.providerProxies = proxies
	gb.listGen++

	return proxies, gb.listGen
}

// ProxiesChanged reports whether a provider of the group published a new proxy
// list since the last GetProxies call. It compares the versions only, so a
// caller that asks on every schedule pays no rebuild.
func (gb *GroupBase) ProxiesChanged() bool {
	gb.getProxiesMutex.Lock()
	defer gb.getProxiesMutex.Unlock()

	if len(gb.providerVersions) != len(gb.providers) {
		return true
	}
	for i, pd := range gb.providers {
		if gb.providerVersions[i] != pd.Version() {
			return true
		}
	}
	return false
}

// URLTest measures every member of the group. The probes go through the shared
// probe queue, so a group test no longer adds its own fan-out on top of the
// checks of every other group; the member timeout is the only budget of a
// probe, and the caller's context bounds only how long the caller waits for the
// answer.
func (gb *GroupBase) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	proxies := gb.GetProxies(false)
	endCheck := beginCheckScope(gb.Name(), url, proxies)
	defer endCheck()

	probes := make([]probequeue.Probe, 0, len(proxies))
	for _, proxy := range proxies {
		probes = append(probes, probequeue.Probe{
			Proxy:    proxy,
			URL:      url,
			Expected: expectedStatus,
			Timeout:  time.Duration(gb.testTimeout) * time.Millisecond,
		})
	}

	var lock sync.Mutex
	mp := map[string]uint16{}
	onResult := func(result probequeue.Result) {
		if result.Err != nil {
			return
		}
		lock.Lock()
		mp[result.Name] = result.Delay
		lock.Unlock()
	}

	// A group with no member measured for this URL has nothing the next
	// connection could route by: its test goes ahead of the background sweeps,
	// so a dial waiting for a first answer is not queued behind them. Once a
	// member answers, the remaining probes drop back to the foreground lane.
	var check *probequeue.Check
	if hasMeasuredMember(proxies, url) {
		check = probequeue.SubmitForeground(gb.Name(), probes, onResult)
	} else {
		check = probequeue.SubmitUrgent(gb.Name(), true, probes, onResult)
	}
	check.Wait(ctx)

	// Copy the measured members: a check the caller left behind keeps probing
	// and must not write the map the caller returns.
	lock.Lock()
	measured := make(map[string]uint16, len(mp))
	for name, delay := range mp {
		measured[name] = delay
	}
	lock.Unlock()

	if len(measured) == 0 {
		return measured, fmt.Errorf("get delay: all proxies timeout")
	}
	return measured, nil
}

func (gb *GroupBase) onDialFailed(adapterType C.AdapterType, err error, fn func()) {
	if adapterType == C.Direct || adapterType == C.Compatible || adapterType == C.Reject || adapterType == C.Pass || adapterType == C.RejectDrop {
		return
	}

	if errors.Is(err, C.ErrNotSupport) {
		return
	}

	go func() {
		if strings.Contains(err.Error(), "connection refused") {
			fn()
			return
		}

		// The counter is decided under the mutex and the check runs after it is
		// released: healthCheck and onDialSuccess take the same mutex, so a
		// check called while it is held waits on itself and blocks every dial
		// of the group behind it.
		activate := false
		gb.failedTestMux.Lock()
		gb.failedTimes++
		if gb.failedTimes == 1 {
			log.Debugln("ProxyGroup: %s first failed", gb.Name())
			gb.failedTime = time.Now()
		} else if time.Since(gb.failedTime) > time.Duration(gb.testTimeout)*time.Millisecond {
			gb.failedTimes = 0
		} else {
			log.Debugln("ProxyGroup: %s failed count: %d", gb.Name(), gb.failedTimes)
			activate = gb.failedTimes >= gb.maxFailedTimes
		}
		gb.failedTestMux.Unlock()

		if activate {
			log.Warnln("because %s failed multiple times, activate health check", gb.Name())
			fn()
		}
	}()
}

func (gb *GroupBase) healthCheck() {
	if gb.failedTesting.Load() {
		return
	}

	gb.failedTesting.Store(true)
	wg := sync.WaitGroup{}
	for _, proxyProvider := range gb.providers {
		wg.Add(1)
		proxyProvider := proxyProvider
		go func() {
			defer wg.Done()
			proxyProvider.HealthCheck()
		}()
	}

	wg.Wait()
	gb.failedTesting.Store(false)
	gb.failedTestMux.Lock()
	gb.failedTimes = 0
	gb.failedTestMux.Unlock()
}

func (gb *GroupBase) onDialSuccess() {
	if !gb.failedTesting.Load() {
		gb.failedTestMux.Lock()
		gb.failedTimes = 0
		gb.failedTestMux.Unlock()
	}
}
