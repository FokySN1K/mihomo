package resolver

import (
	"net/netip"

	"github.com/metacubex/mihomo/common/atomic"
)

// hostMapper is read on every packet that carries an address, while an apply
// may replace it on another goroutine: an interface read that tears is a
// crash, so the mapper lives behind an atomic.
var hostMapper atomic.TypedValue[Enhancer]

// SetHostMapper installs the mapper an apply built.
func SetHostMapper(mapper Enhancer) {
	hostMapper.Store(mapper)
}

// HostMapper returns the mapper an apply installed, nil before the first one.
func HostMapper() Enhancer {
	mapper, _ := hostMapper.LoadOk()
	return mapper
}

type Enhancer interface {
	FakeIPEnabled() bool
	MappingEnabled() bool
	IsFakeIP(netip.Addr) bool
	IsFakeBroadcastIP(netip.Addr) bool
	IsExistFakeIP(netip.Addr) bool
	FindHostByIP(netip.Addr) (string, bool)
	FlushFakeIP() error
	InsertHostByIP(netip.Addr, string)
	StoreFakePoolState()
}

func FakeIPEnabled() bool {
	if mapper := HostMapper(); mapper != nil {
		return mapper.FakeIPEnabled()
	}

	return false
}

func MappingEnabled() bool {
	if mapper := HostMapper(); mapper != nil {
		return mapper.MappingEnabled()
	}

	return false
}

func IsFakeIP(ip netip.Addr) bool {
	if mapper := HostMapper(); mapper != nil {
		return mapper.IsFakeIP(ip)
	}

	return false
}

func IsFakeBroadcastIP(ip netip.Addr) bool {
	if mapper := HostMapper(); mapper != nil {
		return mapper.IsFakeBroadcastIP(ip)
	}

	return false
}

func IsExistFakeIP(ip netip.Addr) bool {
	if mapper := HostMapper(); mapper != nil {
		return mapper.IsExistFakeIP(ip)
	}

	return false
}

func InsertHostByIP(ip netip.Addr, host string) {
	if mapper := HostMapper(); mapper != nil {
		mapper.InsertHostByIP(ip, host)
	}
}

func FindHostByIP(ip netip.Addr) (string, bool) {
	if mapper := HostMapper(); mapper != nil {
		return mapper.FindHostByIP(ip)
	}

	return "", false
}

func FlushFakeIP() error {
	if mapper := HostMapper(); mapper != nil {
		return mapper.FlushFakeIP()
	}
	return nil
}

func StoreFakePoolState() {
	if mapper := HostMapper(); mapper != nil {
		mapper.StoreFakePoolState()
	}
}
