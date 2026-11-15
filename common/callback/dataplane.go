package callback

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	C "github.com/metacubex/mihomo/constant"
)

// ErrHandshakeUnanswered reports a connection that carried the handshake and
// then stayed silent: the node accepted the first write but never answered it.
// Nothing of the application was transacted at that point, so the connection
// is closed and the caller is expected to retry on another node.
var ErrHandshakeUnanswered = errors.New("handshake accepted without an answer")

// DataPlaneObservable is implemented by a connection that already reports how
// its data plane behaves. A parent group asks this before installing its own
// observer: one dead flow must cost one demotion, not one per wrapping level.
type DataPlaneObservable interface {
	DataPlaneObserved() bool
}

// IsDataPlaneObserved reports whether the connection already carries an
// observer of its data plane.
func IsDataPlaneObserved(conn any) bool {
	_, ok := conn.(DataPlaneObservable)
	return ok
}

// DataPlaneOptions configures a data-plane observer around a proxied
// connection. Every callback runs at most once per connection and never holds
// the observation lock, so the dial path may take its own locks inside.
type DataPlaneOptions struct {
	// Handshake marks the connection as one whose first write is a protocol
	// handshake. Only such a write can arm the reply watchdog: writing a plain
	// request does not prove the remote has to answer quickly.
	Handshake bool
	// FirstByteTimeout bounds how long the first response byte may take after
	// a handshake write. The connection is closed when it does not arrive in
	// time; the reset makes the caller retry instead of hanging on a node that
	// accepted the handshake and went silent.
	FirstByteTimeout time.Duration
	// OnFirstByte reports the delay between the first accepted write and the
	// first byte the remote answered with.
	OnFirstByte func(delay time.Duration)
	// OnDrop reports a connection that is not worth keeping: the peer reset
	// it, ended it before answering, or never answered the handshake. The
	// observer closes the connection itself once the callback returns.
	OnDrop func(err error)
}

// dataPlaneConn observes a proxied connection: the first write, the first byte
// the remote answers with, and the ways a flow can die without the dial ever
// failing. The tunnel keeps the connection while the flow lives, so a node that
// accepted the handshake and went silent is otherwise invisible to the group
// that chose it.
type dataPlaneConn struct {
	C.Conn
	opts DataPlaneOptions

	// written marks the first write the proxy accepted, answered the first
	// byte the remote answered with, shut the client's own close.
	written  atomic.Bool
	answered atomic.Bool
	shut     atomic.Bool
	// startAt is when the first write was accepted, in unix nanoseconds: the
	// first answer is measured from it.
	startAt atomic.Int64

	byteOnce sync.Once
	dropOnce sync.Once

	mu    sync.Mutex
	timer *time.Timer
}

// DataPlaneObserved implements DataPlaneObservable.
func (c *dataPlaneConn) DataPlaneObserved() bool {
	return true
}

// Read implements net.Conn
func (c *dataPlaneConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.noteRead(n, err)
	return n, err
}

// ReadBuffer implements N.ExtendedReader
func (c *dataPlaneConn) ReadBuffer(buffer *buf.Buffer) error {
	err := c.Conn.ReadBuffer(buffer)
	c.noteRead(buffer.Len(), err)
	return err
}

// Write implements net.Conn
func (c *dataPlaneConn) Write(b []byte) (int, error) {
	handshake := c.handshakeStart(b)
	n, err := c.Conn.Write(b)
	c.noteWrite(n, handshake, err)
	return n, err
}

// WriteBuffer implements N.ExtendedWriter
func (c *dataPlaneConn) WriteBuffer(buffer *buf.Buffer) error {
	handshake := c.handshakeStart(buffer.Bytes())
	length := buffer.Len()
	err := c.Conn.WriteBuffer(buffer)
	c.noteWrite(length, handshake, err)
	return err
}

// Close implements net.Conn. A close the client asked for is not a verdict on
// the node, and a flow the client abandons looks exactly like a dead one, so
// the watchdog is disarmed here and nothing is reported.
func (c *dataPlaneConn) Close() error {
	c.shut.Store(true)
	c.stopTimer()
	return c.Conn.Close()
}

// ReaderReplaceable and WriterReplaceable stay false: the raw connection is
// not equivalent to this one while the observer has to see the traffic. The
// relay splices only connections that are both syscall.Conn, which a proxied
// connection never is, so the answer costs no throughput.
func (c *dataPlaneConn) ReaderReplaceable() bool {
	return false
}

func (c *dataPlaneConn) WriterReplaceable() bool {
	return false
}

func (c *dataPlaneConn) Upstream() any {
	return c.Conn
}

// handshakeStart reports whether the write that is about to happen is the
// first one of the connection and whether it looks like a TLS handshake, the
// only start the reply watchdog can treat as "an answer must come soon". The
// payload is inspected before the write consumes it.
func (c *dataPlaneConn) handshakeStart(payload []byte) bool {
	if c.written.Load() || !c.opts.Handshake {
		return false
	}
	return looksLikeTLSHandshake(payload)
}

func (c *dataPlaneConn) noteWrite(n int, handshake bool, err error) {
	if n > 0 && c.written.CompareAndSwap(false, true) {
		c.startAt.Store(time.Now().UnixNano())
		if handshake {
			c.armWatchdog()
		}
	}
	if err != nil {
		// A write the proxy rejected is a dead flow like a reset while
		// reading: the group learns it through the same callback.
		c.drop(err)
	}
}

func (c *dataPlaneConn) noteRead(n int, err error) {
	if n > 0 {
		c.noteAnswer()
		return
	}
	if err == nil || c.shut.Load() {
		return
	}
	switch {
	case errors.Is(err, io.EOF):
		// A peer that ends the connection before answering the handshake is
		// not carrying traffic: the flow the client started cannot finish, no
		// matter how cleanly the proxy relayed the end.
		if c.written.Load() && !c.answered.Load() {
			c.drop(err)
		}
	case errors.Is(err, net.ErrClosed), errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.Canceled):
		// the client's own close or deadline, not a verdict on the node
	default:
		c.drop(err)
	}
}

func (c *dataPlaneConn) noteAnswer() {
	if !c.answered.CompareAndSwap(false, true) {
		return
	}
	c.stopTimer()

	c.byteOnce.Do(func() {
		if c.opts.OnFirstByte == nil {
			return
		}
		start := c.startAt.Load()
		if start == 0 {
			return
		}
		c.opts.OnFirstByte(time.Since(time.Unix(0, start)))
	})
}

func (c *dataPlaneConn) armWatchdog() {
	if c.opts.FirstByteTimeout <= 0 || c.opts.OnDrop == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.timer != nil || c.answered.Load() || c.shut.Load() {
		return
	}
	c.timer = time.AfterFunc(c.opts.FirstByteTimeout, c.onWatchdog)
}

func (c *dataPlaneConn) onWatchdog() {
	if c.answered.Load() || c.shut.Load() {
		return
	}
	c.drop(ErrHandshakeUnanswered)
}

func (c *dataPlaneConn) stopTimer() {
	c.mu.Lock()
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.mu.Unlock()
}

func (c *dataPlaneConn) drop(err error) {
	if c.shut.Load() {
		return
	}
	c.dropOnce.Do(func() {
		c.stopTimer()
		if c.opts.OnDrop != nil {
			c.opts.OnDrop(err)
		}
		_ = c.Conn.Close()
	})
}

// looksLikeTLSHandshake reports whether the payload starts a TLS handshake
// record: the record type 0x16 and a legacy version of TLS 1.0 through 1.3.
// Only such a first write arms the watchdog; a plain request may legitimately
// take longer to answer than a handshake does.
func looksLikeTLSHandshake(payload []byte) bool {
	return len(payload) >= 3 && payload[0] == 0x16 && payload[1] == 0x03 && payload[2] <= 0x04
}

var _ C.Conn = (*dataPlaneConn)(nil)
var _ DataPlaneObservable = (*dataPlaneConn)(nil)

// NewDataPlaneConn wraps a proxied connection with a data-plane observer.
func NewDataPlaneConn(c C.Conn, opts DataPlaneOptions) C.Conn {
	return &dataPlaneConn{Conn: c, opts: opts}
}
