package callback

import (
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

type firstWriteCallBackConn struct {
	C.Conn
	callback func(error)
	written  bool
}

func (c *firstWriteCallBackConn) Write(b []byte) (n int, err error) {
	defer func() {
		if !c.written {
			c.written = true
			c.callback(err)
		}
	}()
	return c.Conn.Write(b)
}

func (c *firstWriteCallBackConn) WriteBuffer(buffer *buf.Buffer) (err error) {
	defer func() {
		if !c.written {
			c.written = true
			c.callback(err)
		}
	}()
	return c.Conn.WriteBuffer(buffer)
}

func (c *firstWriteCallBackConn) Upstream() any {
	return c.Conn
}

func (c *firstWriteCallBackConn) WriterReplaceable() bool {
	return c.written
}

func (c *firstWriteCallBackConn) ReaderReplaceable() bool {
	return true
}

var _ N.ExtendedConn = (*firstWriteCallBackConn)(nil)

// NewFirstWriteCallBackConn wraps a connection and reports the error of its
// first write through callback.
//
// Deprecated: the callback runs as soon as the first write returns, before the
// remote answers, so a write the proxy only buffered locally is reported as a
// success. Use NewDataPlaneConn to judge the connection by the first byte the
// remote answers with.
func NewFirstWriteCallBackConn(c C.Conn, callback func(error)) C.Conn {
	return &firstWriteCallBackConn{
		Conn:     c,
		callback: callback,
		written:  false,
	}
}
