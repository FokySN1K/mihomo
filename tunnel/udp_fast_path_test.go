package tunnel

import (
	"net"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
)

// fakeEnhancer answers the enhanced-mode questions of the tunnel without a
// real fake-ip pool: an address in the fake range exists only if it is listed.
type fakeEnhancer struct {
	fakeIP netip.Prefix
	hosts  map[netip.Addr]string
}

func (e *fakeEnhancer) FakeIPEnabled() bool  { return true }
func (e *fakeEnhancer) MappingEnabled() bool { return true }

func (e *fakeEnhancer) IsFakeIP(ip netip.Addr) bool {
	return e.fakeIP.Contains(ip) && ip != e.fakeIP.Masked().Addr()
}

func (e *fakeEnhancer) IsFakeBroadcastIP(netip.Addr) bool { return false }
func (e *fakeEnhancer) IsExistFakeIP(ip netip.Addr) bool  { return e.IsFakeIP(ip) }

func (e *fakeEnhancer) FindHostByIP(ip netip.Addr) (string, bool) {
	host, existed := e.hosts[ip]
	return host, existed
}

func (e *fakeEnhancer) FlushFakeIP() error                        { return nil }
func (e *fakeEnhancer) InsertHostByIP(ip netip.Addr, host string) { e.hosts[ip] = host }
func (e *fakeEnhancer) StoreFakePoolState()                       {}

type countingSender struct {
	sent    atomic.Int32
	dropped atomic.Int32
}

func (s *countingSender) Send(packet C.PacketAdapter) {
	s.sent.Add(1)
	packet.Drop()
}

func (s *countingSender) Process(C.PacketConn, C.WriteBackProxy)   {}
func (s *countingSender) Close()                                   {}
func (s *countingSender) DoSniff(*C.Metadata) error                { return nil }
func (s *countingSender) AddMapping(*C.Metadata, *C.Metadata)      {}
func (s *countingSender) RestoreReadFrom(ip netip.Addr) netip.Addr { return ip }

type countingPacket struct {
	metadata *C.Metadata
	key      string
	dropped  atomic.Int32
}

func (p *countingPacket) Metadata() *C.Metadata                   { return p.metadata }
func (p *countingPacket) Key() string                             { return p.key }
func (p *countingPacket) Data() []byte                            { return nil }
func (p *countingPacket) Drop()                                   { p.dropped.Add(1) }
func (p *countingPacket) LocalAddr() net.Addr                     { return benchAddr{} }
func (p *countingPacket) WriteBack([]byte, net.Addr) (int, error) { return 0, nil }

func fakeIPPacket(key string, ip string) *countingPacket {
	return &countingPacket{
		metadata: &C.Metadata{
			NetWork: C.UDP,
			Type:    C.SOCKS5,
			SrcIP:   netip.MustParseAddr("10.0.0.2"),
			SrcPort: 4321,
			DstIP:   netip.MustParseAddr(ip),
			DstPort: 443,
		},
		key: key,
	}
}

// TestUDPEstablishedFlowSkipsPrecheck pins the fast path of handleUDPConn: a
// packet of a flow the nat table already knows reaches its sender even when
// the fake-ip record behind its destination is gone, because the flow routed
// itself when it was created. A first packet of an unknown flow with the same
// missing record still gets dropped.
func TestUDPEstablishedFlowSkipsPrecheck(t *testing.T) {
	previous := status.Load()
	OnRunning()
	defer status.Store(previous)

	resolver.SetHostMapper(&fakeEnhancer{
		fakeIP: netip.MustParsePrefix("198.18.0.0/15"),
		hosts:  map[netip.Addr]string{},
	})
	defer resolver.SetHostMapper(nil)

	const establishedKey = "10.0.0.2:4321-established"
	sender := &countingSender{}
	natTable.GetOrCreate(establishedKey, func() C.PacketSender { return sender })
	defer natTable.Delete(establishedKey)

	established := fakeIPPacket(establishedKey, "198.18.0.9")
	handleUDPConn(established)

	if got := sender.sent.Load(); got != 1 {
		t.Fatalf("established flow: sender got %d packets, want 1", got)
	}
	if got := established.dropped.Load(); got != 1 {
		// the sender takes ownership of the packet and recycles it itself
		t.Fatalf("established flow: packet recycled %d times, want 1", got)
	}

	unknown := fakeIPPacket("10.0.0.2:4321-unknown", "198.18.0.10")
	handleUDPConn(unknown)

	if got := unknown.dropped.Load(); got != 1 {
		t.Fatalf("unknown flow: packet dropped %d times, want 1", got)
	}

	// the dropped flow must not leave a sender behind
	if _, ok := natTable.Load(unknown.key); ok {
		t.Fatal("unknown flow: a sender was created for a packet that failed the pre-check")
	}
}
