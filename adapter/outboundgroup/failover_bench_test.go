package outboundgroup

import (
	"context"
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
