package dialer

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubConn records how it was used and never touches the network.
type stubConn struct {
	net.Conn
	closed atomic.Bool
	writes atomic.Int64
}

func (c *stubConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *stubConn) Write(b []byte) (int, error)      { c.writes.Add(int64(len(b))); return len(b), nil }
func (c *stubConn) Close() error                     { c.closed.Store(true); return nil }
func (c *stubConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *stubConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *stubConn) SetDeadline(time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(time.Time) error { return nil }

func newTestTFOConn(dialFn func(context.Context, []byte) (net.Conn, error)) *tfoConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &tfoConn{
		ctx:    ctx,
		cancel: cancel,
		dialed: make(chan struct{}),
		dialFn: dialFn,
	}
}

// TestDialTFOContextFollowsCaller guards the fix for the lazy dial ignoring the
// caller: canceling the request must cancel the pending dial, not leave it
// running for the full DefaultTCPTimeout.
func TestDialTFOContextFollowsCaller(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := dialTFO(parent, net.Dialer{}, "tcp", "192.0.2.1:81")
	if err != nil {
		t.Fatalf("dialTFO: %v", err)
	}
	c := conn.(*tfoConn)

	cancel()
	select {
	case <-c.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("dial context is not derived from the caller context")
	}

	start := time.Now()
	if _, err = conn.Write([]byte("early")); err == nil {
		t.Fatal("write after cancel must fail instead of dialing")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("canceled dial returned after %s, want a prompt error", elapsed)
	}
}

// TestTFOConnCloseDuringDial checks that a socket which finishes dialing after
// the connection was closed is released instead of leaking.
func TestTFOConnCloseDuringDial(t *testing.T) {
	dialStarted := make(chan struct{})
	release := make(chan struct{})
	stub := &stubConn{}

	c := newTestTFOConn(func(ctx context.Context, earlyData []byte) (net.Conn, error) {
		close(dialStarted)
		<-release
		return stub, nil
	})

	done := make(chan error, 1)
	go func() {
		_, err := c.Write([]byte("early"))
		done <- err
	}()

	<-dialStarted
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("write returned %v, want ErrClosedPipe", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dial did not return after Close")
	}

	if !stub.closed.Load() {
		t.Fatal("socket dialed while closing was leaked")
	}
}

// TestTFOConnDialsOnce checks that concurrent writers trigger a single dial and
// that the losers still deliver their payload over the established conn.
func TestTFOConnDialsOnce(t *testing.T) {
	var dials atomic.Int32
	stub := &stubConn{}
	c := newTestTFOConn(func(ctx context.Context, earlyData []byte) (net.Conn, error) {
		dials.Add(1)
		time.Sleep(10 * time.Millisecond) // widen the window for the other writers
		return stub, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Write([]byte("payload")); err != nil {
				t.Errorf("write: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := dials.Load(); got != 1 {
		t.Fatalf("dialed %d times, want 1", got)
	}
	if got, want := stub.writes.Load(), int64(len("payload")*7); got != want {
		t.Fatalf("wrote %d bytes over the socket, want %d (one writer goes as early data)", got, want)
	}
	_ = c.Close()
}

// TestTFOConnReadWaitsForDial checks that Read blocks until the dial finishes
// and then reads from the established conn.
func TestTFOConnReadWaitsForDial(t *testing.T) {
	stub := &stubConn{}
	c := newTestTFOConn(func(ctx context.Context, earlyData []byte) (net.Conn, error) {
		time.Sleep(20 * time.Millisecond)
		return stub, nil
	})

	readDone := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 1))
		readDone <- err
	}()

	time.Sleep(5 * time.Millisecond)
	select {
	case err := <-readDone:
		t.Fatalf("read returned %v before the dial finished", err)
	default:
	}

	if _, err := c.Write([]byte("early")); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case err := <-readDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("read returned %v, want the stub's EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not resume after the dial")
	}
	_ = c.Close()
}

// TestTFOConnConcurrentState is a smoke test for the race detector: write, read,
// deadlines and close all touch the conn state at the same time.
func TestTFOConnConcurrentState(t *testing.T) {
	stub := &stubConn{}
	c := newTestTFOConn(func(ctx context.Context, earlyData []byte) (net.Conn, error) {
		return stub, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); _, _ = c.Write([]byte("x")) }()
		go func() { defer wg.Done(); _, _ = c.Read(make([]byte, 1)) }()
		go func() { defer wg.Done(); _ = c.SetDeadline(time.Now()) }()
	}
	_ = c.Close()
	wg.Wait()

	if !c.closed.Load() {
		t.Fatal("conn must stay closed")
	}
}
