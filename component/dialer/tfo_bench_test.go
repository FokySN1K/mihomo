package dialer

import (
	"context"
	"io"
	"net"
	"testing"
)

// startSinkListener accepts connections and drains them until closed.
func startSinkListener(tb testing.TB) net.Listener {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	tb.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				_, _ = io.Copy(io.Discard, c)
				_ = c.Close()
			}(conn)
		}
	}()
	return l
}

// BenchmarkDialTFO_New measures the per-connection cost of wrapping a dialer
// for lazy TCP fast open, which is the hot path for every tfo: true node.
func BenchmarkDialTFO_New(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		conn, err := dialTFO(context.Background(), net.Dialer{}, "tcp", "127.0.0.1:1")
		if err != nil {
			b.Fatalf("dialTFO: %v", err)
		}
		_ = conn.Close()
	}
}

// BenchmarkTFOConn_Write measures the steady-state write path after the lazy
// dial has already happened.
func BenchmarkTFOConn_Write(b *testing.B) {
	l := startSinkListener(b)
	conn, err := dialTFO(context.Background(), net.Dialer{}, "tcp", l.Addr().String())
	if err != nil {
		b.Fatalf("dialTFO: %v", err)
	}
	defer conn.Close()

	// First write is the early data that triggers the real dial.
	if _, err = conn.Write([]byte("hello")); err != nil {
		b.Fatalf("early data write: %v", err)
	}

	payload := []byte("0123456789abcdef")
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err = conn.Write(payload); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

// BenchmarkTFOConn_WriteParallel checks the state reads under concurrency.
func BenchmarkTFOConn_WriteParallel(b *testing.B) {
	l := startSinkListener(b)
	conn, err := dialTFO(context.Background(), net.Dialer{}, "tcp", l.Addr().String())
	if err != nil {
		b.Fatalf("dialTFO: %v", err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("hello")); err != nil {
		b.Fatalf("early data write: %v", err)
	}

	payload := []byte("0123456789abcdef")
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := conn.Write(payload); err != nil {
				b.Fatalf("write: %v", err)
			}
		}
	})
}
