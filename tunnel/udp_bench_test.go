package tunnel

import (
	"net"
	"net/netip"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

type benchAddr struct{}

func (benchAddr) Network() string { return "udp" }
func (benchAddr) String() string  { return "10.0.0.2:1234" }

type benchPacket struct {
	metadata *C.Metadata
	key      string
}

func (p *benchPacket) Metadata() *C.Metadata                   { return p.metadata }
func (p *benchPacket) Key() string                             { return p.key }
func (p *benchPacket) Data() []byte                            { return nil }
func (p *benchPacket) Drop()                                   {}
func (p *benchPacket) LocalAddr() net.Addr                     { return benchAddr{} }
func (p *benchPacket) WriteBack([]byte, net.Addr) (int, error) { return 0, nil }

type benchSender struct{}

func (benchSender) Send(C.PacketAdapter)                     {}
func (benchSender) Process(C.PacketConn, C.WriteBackProxy)   {}
func (benchSender) Close()                                   {}
func (benchSender) DoSniff(*C.Metadata) error                { return nil }
func (benchSender) AddMapping(*C.Metadata, *C.Metadata)      {}
func (benchSender) RestoreReadFrom(ip netip.Addr) netip.Addr { return ip }

func benchEstablishedPacket(key string) *benchPacket {
	return &benchPacket{
		metadata: &C.Metadata{
			NetWork: C.UDP,
			Type:    C.SOCKS5,
			SrcIP:   netip.MustParseAddr("10.0.0.2"),
			SrcPort: 1234,
			DstIP:   netip.MustParseAddr("1.1.1.1"),
			DstPort: 53,
		},
		key: key,
	}
}

func benchRunning(b *testing.B) {
	b.Helper()

	previous := status.Load()
	OnRunning()
	b.Cleanup(func() {
		status.Store(previous)
	})

	natTable.GetOrCreate("10.0.0.2:1234", func() C.PacketSender { return benchSender{} })
}

// BenchmarkHandleUDPConnEstablished measures one packet of a flow the nat
// table already knows: the fast path resolves nothing and copies nothing.
func BenchmarkHandleUDPConnEstablished(b *testing.B) {
	benchRunning(b)
	var packet C.PacketAdapter = benchEstablishedPacket("10.0.0.2:1234")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		handleUDPConn(packet)
	}
}

// BenchmarkHandleUDPConnEstablishedPrecheck is the shape the same packet had
// before the fast path: a metadata copy, the pre-check and the table compute.
func BenchmarkHandleUDPConnEstablishedPrecheck(b *testing.B) {
	benchRunning(b)
	packet := benchEstablishedPacket("10.0.0.2:1234")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		metadata := packet.Metadata()
		fixMetadata(metadata)

		if err := preHandleMetadata(metadata.Clone()); err != nil {
			b.Fatal(err)
		}

		sender, _ := natTable.GetOrCreate(packet.Key(), func() C.PacketSender {
			return benchSender{}
		})
		sender.Send(packet)
	}
}
