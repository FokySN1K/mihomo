package callback

import (
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	C "github.com/metacubex/mihomo/constant"
)

// benchConn is an in-memory connection that answers and accepts everything at
// once, so the benchmark measures the observer around it and not a transport.
type benchConn struct{}

func (c *benchConn) Read(b []byte) (int, error)          { return len(b), nil }
func (c *benchConn) ReadBuffer(buffer *buf.Buffer) error { return nil }
func (c *benchConn) Write(b []byte) (int, error)         { return len(b), nil }
func (c *benchConn) WriteBuffer(*buf.Buffer) error       { return nil }
func (c *benchConn) Close() error                        { return nil }
func (c *benchConn) LocalAddr() net.Addr                 { return nil }
func (c *benchConn) RemoteAddr() net.Addr                { return nil }
func (c *benchConn) SetDeadline(time.Time) error         { return nil }
func (c *benchConn) SetReadDeadline(time.Time) error     { return nil }
func (c *benchConn) SetWriteDeadline(time.Time) error    { return nil }
func (c *benchConn) Upstream() any                       { return nil }
func (c *benchConn) ReaderReplaceable() bool             { return false }
func (c *benchConn) WriterReplaceable() bool             { return false }
func (c *benchConn) Chains() C.Chain                     { return nil }
func (c *benchConn) ProviderChains() C.Chain             { return nil }
func (c *benchConn) AppendToChains(C.ProxyAdapter)       {}
func (c *benchConn) RemoteDestination() string           { return "" }
func (c *benchConn) NeedHandshake() bool                 { return true }

// BenchmarkDataPlaneRead measures the per-read cost of the observer: once the
// first byte has been answered, a read must be one atomic load and nothing
// more, because the relay runs it on every buffer of every connection.
func BenchmarkDataPlaneRead(b *testing.B) {
	payload := make([]byte, 16*1024)

	b.Run("raw", func(b *testing.B) {
		conn := &benchConn{}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := conn.Read(payload); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("observed", func(b *testing.B) {
		conn := NewDataPlaneConn(&benchConn{}, DataPlaneOptions{
			Handshake:        true,
			FirstByteTimeout: time.Hour,
			OnFirstByte:      func(time.Duration) {},
			OnDrop:           func(error) {},
		})
		// the first read carries the one-shot answer bookkeeping; the
		// steady state is what the relay pays
		if _, err := conn.Read(payload); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := conn.Read(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkDataPlaneWrite measures the per-write cost: only the first write of
// a connection can be a handshake, so the steady state must be one atomic load.
func BenchmarkDataPlaneWrite(b *testing.B) {
	payload := make([]byte, 16*1024)

	b.Run("raw", func(b *testing.B) {
		conn := &benchConn{}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := conn.Write(payload); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("observed", func(b *testing.B) {
		conn := NewDataPlaneConn(&benchConn{}, DataPlaneOptions{
			Handshake:        true,
			FirstByteTimeout: time.Hour,
			OnFirstByte:      func(time.Duration) {},
			OnDrop:           func(error) {},
		})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := conn.Write(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}
