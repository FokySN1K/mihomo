package outboundgroup

import (
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// joinProviderProxies puts a `use` provider named in use-position after that
// many of the group's own proxies, which otherwise all come first.
func (gb *GroupBase) joinProviderProxies(parts [][]C.Proxy) []C.Proxy {
	own := -1
	if len(gb.usePosition) != 0 {
		for i, pd := range gb.providers {
			if pd.VehicleType() == P.Compatible {
				own = i
				break
			}
		}
	}

	var proxies []C.Proxy
	if own < 0 {
		for _, part := range parts {
			proxies = append(proxies, part...)
		}
		return proxies
	}

	named := parts[own]
	before := make([][]C.Proxy, len(named)+1)
	for i, pd := range gb.providers {
		if i == own {
			continue
		}
		position, ok := gb.usePosition[pd.Name()]
		if !ok || position > len(named) {
			position = len(named)
		} else if position < 0 {
			position = 0
		}
		before[position] = append(before[position], parts[i]...)
	}
	for i, part := range before {
		proxies = append(proxies, part...)
		if i < len(named) {
			proxies = append(proxies, named[i])
		}
	}
	return proxies
}

// A check picks its own candidates, so the per-probe start messages alone
// cannot tell a host which members are queued for it: a pool checks its own
// rotation, an ordered group checks every member. These hooks report the whole
// set when a check begins and when it is over, whatever it measured.
//
// They live here rather than in the adapter package that owns the delay hooks,
// because the groups are built from adapter and cannot import it back.
var (
	// CheckStartHook reports the members a check is about to measure.
	CheckStartHook func(name string, url string, proxies []C.Proxy)
	// CheckEndHook reports that the check has ended.
	CheckEndHook func(name string, url string)
)

// beginCheckScope reports a check over proxies and returns the function that
// closes it. A check that has nothing to measure opens no scope, so its end
// must not be reported either.
func beginCheckScope(name, url string, proxies []C.Proxy) func() {
	if CheckStartHook == nil || len(proxies) == 0 {
		return func() {}
	}
	CheckStartHook(name, url, proxies)
	if CheckEndHook == nil {
		return func() {}
	}
	return func() { CheckEndHook(name, url) }
}
