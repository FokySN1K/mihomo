package dialer

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/tfo-go"
)

var DisableTFO = false

// tfoConn is a lazily dialed connection: the socket is opened on the first
// Write so the first bytes can ride in the SYN packet. Callers get the conn
// before the dial happens, so its state is read from other goroutines and must
// stay race-free.
type tfoConn struct {
	ctx    context.Context
	cancel context.CancelFunc
	dialFn func(ctx context.Context, earlyData []byte) (net.Conn, error)

	dialed chan struct{}
	closed atomic.Bool
	conn   atomic.Pointer[net.Conn]

	dialMu  sync.Mutex // makes the first Write the only one that dials
	stateMu sync.Mutex // orders Close against committing a freshly dialed conn
}

// Dial performs the lazy dial. It reports whether this call is the one that
// dialed (and therefore consumed earlyData), so a losing concurrent Write can
// send its payload over the established conn instead of dropping it.
func (c *tfoConn) Dial(earlyData []byte) (bool, error) {
	c.dialMu.Lock()
	defer c.dialMu.Unlock()

	if c.closed.Load() {
		return false, io.ErrClosedPipe
	}
	if c.conn.Load() != nil {
		return false, nil
	}

	conn, err := c.dialFn(c.ctx, earlyData)
	if err != nil {
		return false, err
	}

	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.closed.Load() {
		_ = conn.Close()
		return false, io.ErrClosedPipe
	}
	c.conn.Store(&conn)
	close(c.dialed)
	return true, nil
}

func (c *tfoConn) Read(b []byte) (n int, err error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	conn := c.conn.Load()
	if conn == nil {
		select {
		case <-c.ctx.Done():
			return 0, io.ErrUnexpectedEOF
		case <-c.dialed:
		}
		conn = c.conn.Load()
		if conn == nil {
			return 0, io.ErrUnexpectedEOF
		}
	}
	return (*conn).Read(b)
}

func (c *tfoConn) Write(b []byte) (n int, err error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if conn := c.conn.Load(); conn != nil {
		return (*conn).Write(b)
	}

	dialed, err := c.Dial(b)
	if err != nil {
		return 0, err
	}
	if dialed { // early data went out with the handshake
		return len(b), nil
	}
	if conn := c.conn.Load(); conn != nil {
		return (*conn).Write(b)
	}
	return 0, io.ErrClosedPipe
}

func (c *tfoConn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.cancel()

	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if conn := c.conn.Load(); conn != nil {
		return (*conn).Close()
	}
	return nil
}

func (c *tfoConn) LocalAddr() net.Addr {
	if conn := c.conn.Load(); conn != nil {
		return (*conn).LocalAddr()
	}
	return &net.TCPAddr{}
}

func (c *tfoConn) RemoteAddr() net.Addr {
	if conn := c.conn.Load(); conn != nil {
		return (*conn).RemoteAddr()
	}
	return &net.TCPAddr{}
}

func (c *tfoConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *tfoConn) SetReadDeadline(t time.Time) error {
	if conn := c.conn.Load(); conn != nil {
		return (*conn).SetReadDeadline(t)
	}
	return nil
}

func (c *tfoConn) SetWriteDeadline(t time.Time) error {
	if conn := c.conn.Load(); conn != nil {
		return (*conn).SetWriteDeadline(t)
	}
	return nil
}

func (c *tfoConn) Upstream() any {
	if conn := c.conn.Load(); conn != nil { // ensure return a nil interface not an interface with nil value
		return *conn
	}
	return nil
}

func (c *tfoConn) NeedAdditionalReadDeadline() bool {
	return c.conn.Load() == nil
}

func (c *tfoConn) NeedHandshake() bool {
	return c.conn.Load() == nil
}

func (c *tfoConn) ReaderReplaceable() bool {
	return c.conn.Load() != nil
}

func (c *tfoConn) WriterReplaceable() bool {
	return c.conn.Load() != nil
}

func dialTFO(ctx context.Context, netDialer net.Dialer, network, address string) (net.Conn, error) {
	// Keep the caller's budget: when the request is canceled or times out, the
	// lazy dial has to stop as well, otherwise an aborted request keeps dialing
	// in the background for the whole DefaultTCPTimeout.
	ctx, cancel := context.WithTimeout(ctx, DefaultTCPTimeout)
	dialer := tfo.Dialer{Dialer: netDialer, DisableTFO: false}
	return &tfoConn{
		dialed: make(chan struct{}),
		cancel: cancel,
		ctx:    ctx,
		dialFn: func(ctx context.Context, earlyData []byte) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address, earlyData)
		},
	}, nil
}
