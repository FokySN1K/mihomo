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

type SelectAble interface {
	Set(string) error
	ForceSet(name string)
}

var _ SelectAble = (*Fallback)(nil)
var _ SelectAble = (*URLTest)(nil)
var _ SelectAble = (*URLTestPool)(nil)
var _ SelectAble = (*Cascade)(nil)
var _ SelectAble = (*Selector)(nil)
