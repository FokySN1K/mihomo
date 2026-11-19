package nat

import (
	"net/netip"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

type benchSender struct{}

func (benchSender) Send(C.PacketAdapter)                     {}
func (benchSender) Process(C.PacketConn, C.WriteBackProxy)   {}
func (benchSender) Close()                                   {}
func (benchSender) DoSniff(*C.Metadata) error                { return nil }
func (benchSender) AddMapping(*C.Metadata, *C.Metadata)      {}
func (benchSender) RestoreReadFrom(ip netip.Addr) netip.Addr { return ip }

func BenchmarkTableLoadHit(b *testing.B) {
	table := New()
	table.GetOrCreate("10.0.0.2:1234", func() C.PacketSender { return benchSender{} })
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, ok := table.Load("10.0.0.2:1234"); !ok {
			b.Fatal("entry missing")
		}
	}
}

func BenchmarkTableLoadMiss(b *testing.B) {
	table := New()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, ok := table.Load("10.0.0.2:1234"); ok {
			b.Fatal("unexpected entry")
		}
	}
}

func BenchmarkTableGetOrCreateHit(b *testing.B) {
	table := New()
	table.GetOrCreate("10.0.0.2:1234", func() C.PacketSender { return benchSender{} })
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, loaded := table.GetOrCreate("10.0.0.2:1234", func() C.PacketSender { return benchSender{} }); !loaded {
			b.Fatal("entry missing")
		}
	}
}

func BenchmarkTableLoadHitParallel(b *testing.B) {
	table := New()
	table.GetOrCreate("10.0.0.2:1234", func() C.PacketSender { return benchSender{} })
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, ok := table.Load("10.0.0.2:1234"); !ok {
				b.Fatal("entry missing")
			}
		}
	})
}
