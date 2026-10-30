package outboundgroup

import (
	"fmt"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// benchmarkCascadeMembers builds a member list with a realistic mix of alive,
// failed and untested candidates, so the ordering work isn't measured on an
// artificially trivial state map.
func benchmarkCascadeMembers(n int) ([]C.Proxy, map[string]*cascadeState) {
	proxies := make([]C.Proxy, 0, n)
	states := make(map[string]*cascadeState, n)
	now := time.Now()

	for i := 0; i < n; i++ {
		name := fmt.Sprintf("node-%03d", i)
		proxies = append(proxies, newCascadeTestProxy(name))

		switch i % 4 {
		case 0:
			states[name] = &cascadeState{alive: true, delay: 50, checkedAt: now}
		case 1:
			states[name] = &cascadeState{failedUntil: now.Add(time.Minute), checkedAt: now}
		case 2:
			states[name] = &cascadeState{alive: true, delay: 120, checkedAt: now}
		}
	}

	return proxies, states
}

// BenchmarkCascadeCandidateOrder measures the ordering that runs on every dial
// and on every API poll of the group.
func BenchmarkCascadeCandidateOrder(b *testing.B) {
	proxies, states := benchmarkCascadeMembers(500)
	group := &Cascade{states: states}
	group.selected = "node-004"
	group.active = "node-000"
	group.lastActive = "node-008"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if order := group.candidateOrder(proxies); len(order) != len(proxies) {
			b.Fatalf("unexpected order length: %d", len(order))
		}
	}
}

// BenchmarkURLTestPoolProxies measures resolving the pool members against the
// full proxy list, which runs on every dial through the pool.
func BenchmarkURLTestPoolProxies(b *testing.B) {
	proxies := make([]C.Proxy, 0, 1000)
	for i := 0; i < 1000; i++ {
		proxies = append(proxies, newCascadeTestProxy(fmt.Sprintf("node-%03d", i)))
	}

	names := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		names = append(names, fmt.Sprintf("node-%03d", i*37))
	}
	pool := &URLTestPool{pool: names}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if got := pool.poolProxiesLocked(proxies); len(got) != len(names) {
			b.Fatalf("unexpected pool size: %d", len(got))
		}
	}
}
