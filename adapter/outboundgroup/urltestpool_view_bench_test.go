package outboundgroup

import (
	"strconv"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

// newBenchMembers builds a list of the given size whose members dial at once,
// so the benchmark measures the group and not a transport.
func newBenchMembers(size int) []C.Proxy {
	members := make([]C.Proxy, size)
	for i := range members {
		members[i] = newBenchMember("node-"+strconv.Itoa(i), false)
	}
	return members
}

// BenchmarkURLTestPoolFastView measures the choice a dial pays on every
// connection once the view of the pool is published: the names are resolved
// against the list once, and every later choice reuses the answer.
func BenchmarkURLTestPoolFastView(b *testing.B) {
	for _, size := range []int{62, 500} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			pool := newBenchPool(b, newBenchMembers(size)...)
			if pool.fast(false) == nil {
				b.Fatal("no member")
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if pool.fast(false) == nil {
					b.Fatal("no member")
				}
			}
		})
	}
}

// BenchmarkURLTestPoolFastRebuild measures the same choice in the shape it had
// before the view: every choice resolved the pool names against the list again
// and built its maps for that one pick. The cost of the fix is read from one
// run of the benchmark instead of two builds.
func BenchmarkURLTestPoolFastRebuild(b *testing.B) {
	for _, size := range []int{62, 500} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			pool := newBenchPool(b, newBenchMembers(size)...)
			proxies := pool.GetProxies(false)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pool.mu.Lock()
				members := pool.poolProxiesLocked(proxies)
				proxy, _, _ := pool.pickFastLocked(proxies, members, false, false)
				pool.mu.Unlock()
				if proxy == nil {
					b.Fatal("no member")
				}
			}
		})
	}
}

// BenchmarkURLTestPoolFastViewParallel measures the choice while many flows ask
// at once: the published view is read and nothing is built per flow.
func BenchmarkURLTestPoolFastViewParallel(b *testing.B) {
	for _, size := range []int{62, 500} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			pool := newBenchPool(b, newBenchMembers(size)...)
			if pool.fast(false) == nil {
				b.Fatal("no member")
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if pool.fast(false) == nil {
						// b.Fatal is not allowed from the goroutine of a parallel
						// body; the case never happens, it only guards the harness
						panic("no member")
					}
				}
			})
		})
	}
}

// BenchmarkURLTestPoolFastViewUDP measures the packet choice on a mixed pool:
// the UDP capability is read live, so this is the cost a packet flow pays on
// top of the published view.
func BenchmarkURLTestPoolFastViewUDP(b *testing.B) {
	pool := newBenchPool(b, newBenchMembers(62)...)
	if proxy, _ := pool.fastForPacket(false); proxy == nil {
		b.Fatal("no member")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if proxy, _ := pool.fastForPacket(false); proxy == nil {
			b.Fatal("no member")
		}
	}
}
