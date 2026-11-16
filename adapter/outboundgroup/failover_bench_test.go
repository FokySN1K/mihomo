package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/common/buf"
	"github.com/metacubex/mihomo/component/reputation"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"

	"github.com/stretchr/testify/require"
)

// benchConn is enough of a connection for the dial path: the benchmark
// measures how a group hands a connection out, no bytes are exchanged. The
// handshake flag makes it the connection of a protocol whose dial only accepts
// the route, like VLESS/REALITY.
type benchConn struct {
	handshake bool
}

func (c *benchConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *benchConn) ReadBuffer(*buf.Buffer) error     { return io.EOF }
func (c *benchConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *benchConn) WriteBuffer(*buf.Buffer) error    { return nil }
func (c *benchConn) Close() error                     { return nil }
func (c *benchConn) LocalAddr() net.Addr              { return nil }
func (c *benchConn) RemoteAddr() net.Addr             { return nil }
func (c *benchConn) SetDeadline(time.Time) error      { return nil }
func (c *benchConn) SetReadDeadline(time.Time) error  { return nil }
func (c *benchConn) SetWriteDeadline(time.Time) error { return nil }
func (c *benchConn) Upstream() any                    { return nil }
func (c *benchConn) ReaderReplaceable() bool          { return false }
func (c *benchConn) WriterReplaceable() bool          { return false }
func (c *benchConn) Chains() C.Chain                  { return nil }
func (c *benchConn) ProviderChains() C.Chain          { return nil }
func (c *benchConn) AppendToChains(C.ProxyAdapter)    {}
func (c *benchConn) RemoteDestination() string        { return "" }
func (c *benchConn) NeedHandshake() bool              { return c.handshake }

// newBenchMember is a member whose dial always connects at once, so the
// benchmark measures the group around the dial and nothing of a transport.
func newBenchMember(name string, handshake bool) C.Proxy {
	base := outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Shadowsocks, UDP: true})
	return adapter.NewProxy(&poolFakeAdapter{Base: base, dialFn: func(context.Context, *C.Metadata) (C.Conn, error) {
		return &benchConn{handshake: handshake}, nil
	}})
}

// newBenchPool mirrors newPoolForTest for a benchmark: the scheduler is frozen,
// so the dial path is measured instead of a health check racing it.
func newBenchPool(b *testing.B, proxies ...C.Proxy) *URLTestPool {
	b.Helper()

	pd, err := provider.NewCompatibleProvider("bench-pool", proxies,
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(b, err)

	pool, err := NewURLTestPool(GroupCommonOption{Name: "bench-pool", Interval: 300},
		URLTestPoolOption{AttemptTimeout: intPtr(1500)}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(b, err)
	pool.reputation = reputation.NewStore()
	pool.testUrl = "http://127.0.0.1:1/"
	pool.pool = cascadeProxyNames(proxies)
	pool.lastPool = append([]string(nil), pool.pool...)
	pool.hasFullCheck.Store(true)
	pool.lastFull.Store(time.Now())
	pool.lastQuick.Store(time.Now())
	return pool
}

func benchmarkPoolDial(b *testing.B, handshake bool) {
	pool := newBenchPool(b,
		newBenchMember("a", handshake),
		newBenchMember("b", handshake),
		newBenchMember("c", handshake),
		newBenchMember("d", handshake),
	)
	metadata := &C.Metadata{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := pool.DialContext(context.Background(), metadata)
		if err != nil {
			b.Fatal(err)
		}
		_ = conn.Close()
	}
}

// BenchmarkURLTestPoolDial measures a dial of a protocol whose own dial already
// proves the connection (QUIC, an authentication exchange).
func BenchmarkURLTestPoolDial(b *testing.B) {
	benchmarkPoolDial(b, false)
}

// BenchmarkURLTestPoolDialHandshake measures a dial of a protocol whose dial
// only accepts the route (VLESS/REALITY): the member is credited at its first
// answered byte, and the connection is observed.
func BenchmarkURLTestPoolDialHandshake(b *testing.B) {
	benchmarkPoolDial(b, true)
}

// BenchmarkURLTestPoolFast measures the member choice a packet flow pays on
// every write.
func BenchmarkURLTestPoolFast(b *testing.B) {
	pool := newBenchPool(b,
		newBenchMember("a", false),
		newBenchMember("b", false),
		newBenchMember("c", false),
		newBenchMember("d", false),
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if pool.fast(false) == nil {
			b.Fatal("no member")
		}
	}
}

func newBenchCascade(b *testing.B, proxies ...C.Proxy) *Cascade {
	b.Helper()

	pd, err := provider.NewCompatibleProvider("bench-cascade", proxies,
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(b, err)

	group, err := NewCascade(GroupCommonOption{Name: "bench-cascade", Interval: 300},
		CascadeOption{}, newPoolEmptyFallback(), []P.ProxyProvider{pd})
	require.NoError(b, err)
	group.lastCheck.Store(time.Now())
	return group
}

// BenchmarkCascadeDial measures a dial through the priority order of a cascade
// for a protocol whose own dial already proves the connection (QUIC).
func BenchmarkCascadeDial(b *testing.B) {
	benchmarkCascadeDial(b, false)
}

// BenchmarkCascadeDialHandshake measures a dial of a candidate whose dial only
// accepts the route (VLESS/REALITY).
func BenchmarkCascadeDialHandshake(b *testing.B) {
	benchmarkCascadeDial(b, true)
}

func benchmarkCascadeDial(b *testing.B, handshake bool) {
	group := newBenchCascade(b,
		newBenchMember("a", handshake),
		newBenchMember("b", handshake),
		newBenchMember("c", handshake),
		newBenchMember("d", handshake),
	)
	metadata := &C.Metadata{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := group.DialContext(context.Background(), metadata)
		if err != nil {
			b.Fatal(err)
		}
		_ = conn.Close()
	}
}

// BenchmarkURLTestPoolOrderForCheck measures the ordering a full sweep pays
// for every candidate of a large provider, with the marks and the reputation a
// real outage leaves behind.
func BenchmarkURLTestPoolOrderForCheck(b *testing.B) {
	b.Run("62", func(b *testing.B) { benchmarkOrderForCheck(b, 62) })
	b.Run("500", func(b *testing.B) { benchmarkOrderForCheck(b, 500) })
}

// BenchmarkURLTestPoolOrderForCheckBefore measures the order the pool used
// before the marks and the transport coverage on the same list, so the cost of
// the fix is read from one run of the benchmark instead of two builds.
func BenchmarkURLTestPoolOrderForCheckBefore(b *testing.B) {
	b.Run("62", func(b *testing.B) { benchmarkOrderForCheckBefore(b, 62) })
	b.Run("500", func(b *testing.B) { benchmarkOrderForCheckBefore(b, 500) })
}

func newBenchOrderMembers(b *testing.B, size int) (*URLTestPool, []C.Proxy) {
	b.Helper()

	// the setup marks the members, and the warning of every mark would bury
	// the benchmark line it belongs to
	previous := log.Level()
	log.SetLevel(log.ERROR)
	b.Cleanup(func() { log.SetLevel(previous) })

	members := make([]C.Proxy, 0, size)
	for i := 0; i < size; i++ {
		members = append(members, newPoolOrderedProxy(fmt.Sprintf("p%03d", i), false, 0))
	}
	pool := newBenchPool(b, members...)
	for i := 0; i < size/2; i++ {
		name := members[i].Name()
		for n := 0; n < 5; n++ {
			pool.reputation.DialSucceeded(name)
		}
		pool.onDataSilent(members[i], errors.New("no answer"))
	}
	return pool, members
}

func benchmarkOrderForCheck(b *testing.B, size int) {
	pool, members := newBenchOrderMembers(b, size)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ordered := pool.orderForCheck(members); len(ordered) != len(members) {
			b.Fatal("the sweep lost members")
		}
	}
}

func benchmarkOrderForCheckBefore(b *testing.B, size int) {
	pool, members := newBenchOrderMembers(b, size)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ordered := orderForCheckBeforeMarks(pool, members); len(ordered) != len(members) {
			b.Fatal("the sweep lost members")
		}
	}
}

// BenchmarkURLTestPoolBlindDialFallback measures the member choice of a dial
// that has nothing measured alive, with every member of a large provider
// marked: the fallback scans the list for the member whose mark ends first.
func BenchmarkURLTestPoolBlindDialFallback(b *testing.B) {
	members := make([]C.Proxy, 0, 62)
	for i := 0; i < 62; i++ {
		members = append(members, newBenchMember(fmt.Sprintf("p%02d", i), false))
	}
	pool := newBenchPool(b, members...)

	until := time.Now().Add(time.Minute)
	pool.mu.Lock()
	for _, proxy := range members {
		pool.suspicious[proxy.Name()] = until
	}
	pool.lastGood = members[0].Name()
	pool.mu.Unlock()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if pool.blindDialFallback(members) == nil {
			b.Fatal("no fallback member")
		}
	}
}
