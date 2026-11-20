package callback

import (
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

// dataFakeConn is a connection a test drives by hand: the test decides which
// bytes the remote answers with and when, so the observer can be watched
// without a network.
type dataFakeConn struct {
	reads  chan fakeRead
	closed chan struct{}
	once   sync.Once

	mu     sync.Mutex
	writes [][]byte
}

type fakeRead struct {
	data []byte
	err  error
}

func newDataFakeConn() *dataFakeConn {
	return &dataFakeConn{reads: make(chan fakeRead, 8), closed: make(chan struct{})}
}

func (c *dataFakeConn) answer(data []byte) { c.reads <- fakeRead{data: data} }
func (c *dataFakeConn) fail(err error)     { c.reads <- fakeRead{err: err} }

func (c *dataFakeConn) Read(b []byte) (int, error) {
	select {
	case r := <-c.reads:
		if r.err != nil {
			return 0, r.err
		}
		return copy(b, r.data), nil
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *dataFakeConn) ReadBuffer(buffer *buf.Buffer) error {
	data := make([]byte, buffer.FreeLen())
	n, err := c.Read(data)
	if n > 0 {
		_, _ = buffer.Write(data[:n])
	}
	return err
}

func (c *dataFakeConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), b...))
	c.mu.Unlock()
	return len(b), nil
}

func (c *dataFakeConn) WriteBuffer(buffer *buf.Buffer) error {
	_, err := c.Write(buffer.Bytes())
	return err
}

func (c *dataFakeConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *dataFakeConn) LocalAddr() net.Addr              { return nil }
func (c *dataFakeConn) RemoteAddr() net.Addr             { return nil }
func (c *dataFakeConn) SetDeadline(time.Time) error      { return nil }
func (c *dataFakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *dataFakeConn) SetWriteDeadline(time.Time) error { return nil }
func (c *dataFakeConn) Chains() C.Chain                  { return nil }
func (c *dataFakeConn) ProviderChains() C.Chain          { return nil }
func (c *dataFakeConn) AppendToChains(C.ProxyAdapter)    {}
func (c *dataFakeConn) RemoteDestination() string        { return "" }

// tlsClientHello is the start of a TLS handshake record: the only first write
// the watchdog is allowed to treat as "an answer must come soon".
var tlsClientHello = []byte{0x16, 0x03, 0x01, 0x00, 0x2e, 0x01, 0x00, 0x00, 0x2a}

func waitDrop(t *testing.T, dropped <-chan error, why string) error {
	t.Helper()
	select {
	case err := <-dropped:
		return err
	case <-time.After(time.Second):
		t.Fatalf("no drop was reported: %s", why)
		return nil
	}
}

func requireNoDrop(t *testing.T, dropped <-chan error, wait time.Duration, why string) {
	t.Helper()
	select {
	case err := <-dropped:
		t.Fatalf("unexpected drop (%v): %s", err, why)
	case <-time.After(wait):
	}
}

// A node that accepts the handshake and never answers is exactly the flow the
// user waits on after the dial already reported success.
func TestDataPlaneWatchdogClosesSilentHandshake(t *testing.T) {
	inner := newDataFakeConn()
	dropped := make(chan error, 1)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: 60 * time.Millisecond,
		OnDrop:           func(err error) { dropped <- err },
	})

	_, err := conn.Write(tlsClientHello)
	require.NoError(t, err)

	require.ErrorIs(t, waitDrop(t, dropped, "a silent handshake must be reported"), ErrHandshakeUnanswered)

	select {
	case <-inner.closed:
	case <-time.After(time.Second):
		t.Fatal("the silent connection was not closed")
	}
}

// A plain request is not a handshake: the server may take as long as it needs,
// so the watchdog must stay disarmed.
func TestDataPlaneWatchdogIgnoresPlainWrite(t *testing.T) {
	inner := newDataFakeConn()
	dropped := make(chan error, 1)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: 40 * time.Millisecond,
		OnDrop:           func(err error) { dropped <- err },
	})

	_, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example\r\n\r\n"))
	require.NoError(t, err)

	requireNoDrop(t, dropped, 150*time.Millisecond, "a plain request is not a handshake")
}

// The first answered byte ends the watchdog and reports the measured delay.
func TestDataPlaneFirstByteStopsWatchdog(t *testing.T) {
	inner := newDataFakeConn()
	dropped := make(chan error, 1)
	answered := make(chan time.Duration, 1)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: 80 * time.Millisecond,
		OnFirstByte:      func(delay time.Duration) { answered <- delay },
		OnDrop:           func(err error) { dropped <- err },
	})

	_, err := conn.Write(tlsClientHello)
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond)
	inner.answer([]byte("server hello"))

	buffer := make([]byte, 32)
	n, err := conn.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, len("server hello"), n)

	select {
	case delay := <-answered:
		require.GreaterOrEqual(t, delay, 20*time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("the first answer was not reported")
	}
	requireNoDrop(t, dropped, 150*time.Millisecond, "an answered flow must not be cut")
}

// A close the client asked for is not a verdict on the node.
func TestDataPlaneClientCloseIsNotVerdict(t *testing.T) {
	inner := newDataFakeConn()
	dropped := make(chan error, 1)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: 40 * time.Millisecond,
		OnDrop:           func(err error) { dropped <- err },
	})

	_, err := conn.Write(tlsClientHello)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	requireNoDrop(t, dropped, 120*time.Millisecond, "a client close is not a dead node")
}

// A reset while reading is a dead flow and is reported once.
func TestDataPlaneResetReportsDropOnce(t *testing.T) {
	inner := newDataFakeConn()
	dropped := make(chan error, 4)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: time.Second,
		OnDrop:           func(err error) { dropped <- err },
	})

	_, err := conn.Write(tlsClientHello)
	require.NoError(t, err)

	inner.fail(syscall.ECONNRESET)
	_, err = conn.Read(make([]byte, 8))
	require.ErrorIs(t, err, syscall.ECONNRESET)
	require.ErrorIs(t, waitDrop(t, dropped, "a reset must be reported"), syscall.ECONNRESET)

	// every later outcome of the same connection must not report again
	inner.fail(syscall.ECONNRESET)
	_, _ = conn.Read(make([]byte, 8))
	select {
	case err := <-dropped:
		t.Fatalf("the same dead connection was reported twice: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

// A peer that ends the flow before answering is not carrying traffic, however
// cleanly the proxy relayed the end.
func TestDataPlaneEndBeforeAnswerReportsDrop(t *testing.T) {
	inner := newDataFakeConn()
	dropped := make(chan error, 1)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: time.Second,
		OnDrop:           func(err error) { dropped <- err },
	})

	_, err := conn.Write(tlsClientHello)
	require.NoError(t, err)

	inner.fail(io.EOF)
	_, err = conn.Read(make([]byte, 8))
	require.ErrorIs(t, err, io.EOF)
	require.ErrorIs(t, waitDrop(t, dropped, "an end before the answer must be reported"), io.EOF)
}

// A close the protocol relays as an error is the peer saying goodbye, not the
// node going silent: a WebSocket close frame used to demote the member even
// though it carried the whole flow.
func TestDataPlaneProtocolCloseIsNotVerdict(t *testing.T) {
	inner := newDataFakeConn()
	dropped := make(chan error, 1)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: time.Second,
		OnDrop:           func(err error) { dropped <- err },
	})

	_, err := conn.Write(tlsClientHello)
	require.NoError(t, err)

	inner.fail(errors.New("websocket: close 1000 (normal)"))
	_, err = conn.Read(make([]byte, 8))
	require.ErrorContains(t, err, "close 1000")
	requireNoDrop(t, dropped, 100*time.Millisecond, "a protocol-level close says nothing about the node")
}

// A connection whose first write is not a handshake owes no answer, so its end
// cannot be read as a silent node.
func TestDataPlaneEndWithoutHandshakeIsNotVerdict(t *testing.T) {
	inner := newDataFakeConn()
	dropped := make(chan error, 1)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: time.Second,
		OnDrop:           func(err error) { dropped <- err },
	})

	_, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example\r\n\r\n"))
	require.NoError(t, err)

	inner.fail(io.EOF)
	_, err = conn.Read(make([]byte, 8))
	require.ErrorIs(t, err, io.EOF)
	requireNoDrop(t, dropped, 100*time.Millisecond, "a plain request never owed an answer")
}

// A flow that already got its answer is not a silent node: how it ends after
// the first answered byte says nothing about the route.
func TestDataPlaneEndAfterAnswerIsNotVerdict(t *testing.T) {
	inner := newDataFakeConn()
	dropped := make(chan error, 1)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: time.Second,
		OnDrop:           func(err error) { dropped <- err },
	})

	_, err := conn.Write(tlsClientHello)
	require.NoError(t, err)
	inner.answer([]byte("server hello"))
	_, err = conn.Read(make([]byte, 32))
	require.NoError(t, err)

	inner.fail(syscall.ECONNRESET)
	_, err = conn.Read(make([]byte, 8))
	require.ErrorIs(t, err, syscall.ECONNRESET)
	requireNoDrop(t, dropped, 100*time.Millisecond, "the node answered, so the reset is not a verdict")
}

// The observer must work through the buffer API as well: the relay copies with
// ReadBuffer/WriteBuffer, not with Read/Write.
func TestDataPlaneBufferCopiesAreObserved(t *testing.T) {
	inner := newDataFakeConn()
	answered := make(chan time.Duration, 1)
	conn := NewDataPlaneConn(inner, DataPlaneOptions{
		Handshake:        true,
		FirstByteTimeout: 80 * time.Millisecond,
		OnFirstByte:      func(delay time.Duration) { answered <- delay },
	})

	write := buf.As(tlsClientHello)
	require.NoError(t, conn.WriteBuffer(write))

	inner.answer([]byte("ok"))
	read := buf.New()
	defer read.Release()
	require.NoError(t, conn.ReadBuffer(read))
	require.Equal(t, 2, read.Len())

	select {
	case <-answered:
	case <-time.After(time.Second):
		t.Fatal("a buffer copy did not report the answer")
	}
}

func TestDataPlaneObservedMarker(t *testing.T) {
	inner := newDataFakeConn()
	require.False(t, IsDataPlaneObserved(inner), "a plain connection is not observed")

	conn := NewDataPlaneConn(inner, DataPlaneOptions{})
	require.True(t, IsDataPlaneObserved(conn), "the wrapper is what a parent group must not wrap again")
}
