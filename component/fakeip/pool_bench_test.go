package fakeip

import (
	"net/netip"
	"testing"
)

// lookBackLocked is the shape LookBack had before the prefix gate: every
// packet took the pool lock before asking the store.
func lookBackLocked(p *Pool, ip netip.Addr) (string, bool) {
	p.mux.Lock()
	defer p.mux.Unlock()

	return p.store.GetByIP(ip)
}

func benchPool(b *testing.B, ipnet string) (*Pool, netip.Addr) {
	b.Helper()

	pool, err := New(Options{
		IPNet: netip.MustParsePrefix(ipnet),
		Size:  4096,
	})
	if err != nil {
		b.Fatal(err)
	}
	ip := pool.Lookup("bench.example.com")
	return pool, ip
}

func BenchmarkLookBackHit(b *testing.B) {
	pool, ip := benchPool(b, "198.18.0.0/15")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, existed := pool.LookBack(ip); !existed {
			b.Fatal("entry missing")
		}
	}
}

func BenchmarkLookBackHitLocked(b *testing.B) {
	pool, ip := benchPool(b, "198.18.0.0/15")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, existed := lookBackLocked(pool, ip); !existed {
			b.Fatal("entry missing")
		}
	}
}

func BenchmarkLookBackMiss(b *testing.B) {
	pool, _ := benchPool(b, "198.18.0.0/15")
	ip := netip.MustParseAddr("8.8.8.8")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, existed := pool.LookBack(ip); existed {
			b.Fatal("unexpected entry")
		}
	}
}

func BenchmarkLookBackMissLocked(b *testing.B) {
	pool, _ := benchPool(b, "198.18.0.0/15")
	ip := netip.MustParseAddr("8.8.8.8")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lookBackLocked(pool, ip)
	}
}

func BenchmarkLookBackHitParallel(b *testing.B) {
	pool, ip := benchPool(b, "198.18.0.0/15")
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, existed := pool.LookBack(ip); !existed {
				b.Fatal("entry missing")
			}
		}
	})
}

func BenchmarkLookBackHitLockedParallel(b *testing.B) {
	pool, ip := benchPool(b, "198.18.0.0/15")
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, existed := lookBackLocked(pool, ip); !existed {
				b.Fatal("entry missing")
			}
		}
	})
}

func BenchmarkExistHit(b *testing.B) {
	pool, ip := benchPool(b, "198.18.0.0/15")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !pool.Exist(ip) {
			b.Fatal("entry missing")
		}
	}
}
