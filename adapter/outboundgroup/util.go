package outboundgroup

import (
	"context"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type ProxyGroup interface {
	C.ProxyAdapter

	Providers() []P.ProxyProvider
	Proxies() []C.Proxy
	Now() string
	Touch()

	Hidden() bool
	Icon() string

	URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (mp map[string]uint16, err error)
}

var _ ProxyGroup = (*Fallback)(nil)
var _ ProxyGroup = (*LoadBalance)(nil)
var _ ProxyGroup = (*URLTest)(nil)
var _ ProxyGroup = (*URLTestPool)(nil)
var _ ProxyGroup = (*Cascade)(nil)
var _ ProxyGroup = (*Selector)(nil)

// SelfTester is a group that checks itself against its own target instead of
// letting a host dial through it. Testing such a group through a single dial
// exercises one member only, and a group that keeps a pool or an ordered
// fallback must run its own check to re-decide what it routes to.
type SelfTester interface {
	SelfTestTarget() (url string, expectedStatus utils.IntRanges[uint16])
}

var _ SelfTester = (*Cascade)(nil)
var _ SelfTester = (*URLTestPool)(nil)
var _ SelfTester = (*LoadBalance)(nil)

// Warmer is a group that measures its own members and can be asked to take the
// first measurement before any traffic arrives. A group that inherits an
// always-on provider health check needs nothing here; one that keeps the
// provider ticker off would otherwise measure only on the first dial.
type Warmer interface {
	WarmUp()
}

var _ Warmer = (*URLTestPool)(nil)

type SelectAble interface {
	Set(string) error
	ForceSet(name string)
}

var _ SelectAble = (*Fallback)(nil)
var _ SelectAble = (*URLTest)(nil)
var _ SelectAble = (*URLTestPool)(nil)
var _ SelectAble = (*Cascade)(nil)
var _ SelectAble = (*Selector)(nil)

// measuredURLDelay is the delay a proxy reports for a URL it was never measured
// on. It is the absence of a measurement, not a very slow one.
const measuredURLDelay = 0xffff

// hasMeasuredMember reports whether the group holds a member that was measured
// for this URL. AliveForTestUrl cannot answer it: a proxy that was never tested
// reports itself alive, so the delay of the URL is the evidence.
func hasMeasuredMember(proxies []C.Proxy, url string) bool {
	for _, proxy := range proxies {
		if proxy.LastDelayForTestUrl(url) != measuredURLDelay {
			return true
		}
	}
	return false
}
