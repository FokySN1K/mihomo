package outbound

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

type fragmentTestConn struct {
	writes     [][]byte
	failWrites int
	closed     bool
}

func (c *fragmentTestConn) Write(p []byte) (int, error) {
	if c.failWrites > 0 {
		c.failWrites--
		return 0, errors.New("write failed")
	}
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (c *fragmentTestConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *fragmentTestConn) Close() error                     { c.closed = true; return nil }
func (c *fragmentTestConn) LocalAddr() net.Addr              { return nil }
func (c *fragmentTestConn) RemoteAddr() net.Addr             { return nil }
func (c *fragmentTestConn) SetDeadline(time.Time) error      { return nil }
func (c *fragmentTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fragmentTestConn) SetWriteDeadline(time.Time) error { return nil }

func testClientHello(host string) []byte {
	name := []byte(host)
	listBodyLen := 3 + len(name)
	list := []byte{byte(listBodyLen >> 8), byte(listBodyLen), 0x00, byte(len(name) >> 8), byte(len(name))}
	list = append(list, name...)

	ext := []byte{0x00, 0x00, byte(len(list) >> 8), byte(len(list))}
	ext = append(ext, list...)

	body := []byte{0x03, 0x03}
	body = append(body, make([]byte, 32)...)
	body = append(body, 0x00)                   // session id
	body = append(body, 0x00, 0x02, 0x13, 0x01) // cipher suites
	body = append(body, 0x01, 0x00)             // compression
	body = append(body, byte(len(ext)>>8), byte(len(ext)))
	body = append(body, ext...)

	handshake := []byte{0x01, byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}
	handshake = append(handshake, body...)

	record := []byte{0x16, 0x03, 0x01, byte(len(handshake) >> 8), byte(len(handshake))}
	return append(record, handshake...)
}

func TestFragmentSplitsClientHelloAtSNI(t *testing.T) {
	hello := testClientHello("www.youtube.com")
	raw := &fragmentTestConn{}
	fc := &fragmentConn{
		Conn: raw,
		cfg: fragmentConfig{
			mode:          fragmentModeTLS,
			splitPosition: defaultFragmentSplitPosition,
			splitSNI:      true,
			segments:      2,
		},
	}

	n, err := fc.Write(hello)
	require.NoError(t, err)
	require.Equal(t, len(hello), n)
	require.Len(t, raw.writes, 2)
	require.Equal(t, hello, bytes.Join(raw.writes, nil))

	sni := bytes.Index(hello, []byte("www.youtube.com"))
	require.Positive(t, sni)
	require.Len(t, raw.writes[0], sni+len("www.youtube.com")/2)
}

func TestFragmentSplitsIntoThreeSegments(t *testing.T) {
	hello := testClientHello("youtube.com")
	raw := &fragmentTestConn{}
	fc := &fragmentConn{
		Conn: raw,
		cfg: fragmentConfig{
			mode:          fragmentModeTLS,
			splitPosition: defaultFragmentSplitPosition,
			splitSNI:      true,
			segments:      3,
		},
	}

	_, err := fc.Write(hello)
	require.NoError(t, err)
	require.Len(t, raw.writes, 3)
	require.Equal(t, hello, bytes.Join(raw.writes, nil))
	require.Equal(t, []byte{hello[0]}, raw.writes[0])
}

func TestFragmentPassesPlaintextInTLSMode(t *testing.T) {
	raw := &fragmentTestConn{}
	fc := &fragmentConn{
		Conn: raw,
		cfg:  fragmentConfig{mode: fragmentModeTLS, segments: 2},
	}

	request := []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
	n, err := fc.Write(request)
	require.NoError(t, err)
	require.Equal(t, len(request), n)
	require.Len(t, raw.writes, 1)
	require.Equal(t, request, raw.writes[0])
}

func TestFragmentHTTPModeUsesFixedOffset(t *testing.T) {
	raw := &fragmentTestConn{}
	fc := &fragmentConn{
		Conn: raw,
		cfg: fragmentConfig{
			mode:          fragmentModeHTTP,
			splitPosition: 5,
			segments:      2,
		},
	}

	request := []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
	_, err := fc.Write(request)
	require.NoError(t, err)
	require.Len(t, raw.writes, 2)
	require.Equal(t, request[:5], raw.writes[0])
	require.Equal(t, request, bytes.Join(raw.writes, nil))
}

func TestFragmentTinyWriteIsNotSplit(t *testing.T) {
	raw := &fragmentTestConn{}
	fc := &fragmentConn{
		Conn: raw,
		cfg:  fragmentConfig{mode: fragmentModeAny, splitPosition: 3, segments: 4},
	}

	_, err := fc.Write([]byte{0x16})
	require.NoError(t, err)
	require.Len(t, raw.writes, 1)
}

func TestFragmentFallbackPlainReplaysFirstWrite(t *testing.T) {
	hello := testClientHello("www.youtube.com")
	broken := &fragmentTestConn{failWrites: 1}
	plain := &fragmentTestConn{}

	fc := &fragmentConn{
		Conn: broken,
		cfg: fragmentConfig{
			mode:            fragmentModeTLS,
			splitPosition:   defaultFragmentSplitPosition,
			splitSNI:        true,
			segments:        2,
			fallbackPlain:   true,
			fallbackTimeout: time.Second,
		},
		dial: func(context.Context) (net.Conn, error) { return plain, nil },
	}

	n, err := fc.Write(hello)
	require.NoError(t, err)
	require.Equal(t, len(hello), n)
	require.Empty(t, broken.writes)
	require.Len(t, plain.writes, 1)
	require.Equal(t, hello, plain.writes[0])
	require.True(t, broken.closed)
}

func TestFragmentFallbackPlainDisabled(t *testing.T) {
	hello := testClientHello("www.youtube.com")
	broken := &fragmentTestConn{failWrites: 1}
	dialCalled := false

	fc := &fragmentConn{
		Conn: broken,
		cfg: fragmentConfig{
			mode:          fragmentModeTLS,
			splitPosition: defaultFragmentSplitPosition,
			segments:      2,
		},
		dial: func(context.Context) (net.Conn, error) {
			dialCalled = true
			return &fragmentTestConn{}, nil
		},
	}

	_, err := fc.Write(hello)
	require.Error(t, err)
	require.False(t, dialCalled)
}

func TestTLSClientHelloSNIOffsetRejectsTruncated(t *testing.T) {
	hello := testClientHello("example.com")
	_, ok := tlsClientHelloSNIOffset(hello[:20])
	require.False(t, ok)

	_, ok = tlsClientHelloSNIOffset([]byte("GET / HTTP/1.1\r\n"))
	require.False(t, ok)
}

func TestFragmentDialContextOverTCP(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	payload := bytes.Repeat([]byte("a"), 64)
	received := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, buf); err == nil {
			received <- buf
		}
	}()

	fragment, err := NewFragmentWithOption(FragmentOption{
		Name:          "frag",
		Mode:          fragmentModeAny,
		SplitPosition: 3,
		Segments:      2,
		Delay:         1,
	})
	require.NoError(t, err)

	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	metadata := &C.Metadata{
		DstIP:   netip.MustParseAddr("127.0.0.1"),
		DstPort: port,
	}
	conn, err := fragment.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	defer conn.Close()

	n, err := conn.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	select {
	case got := <-received:
		require.Equal(t, payload, got)
	case <-time.After(2 * time.Second):
		t.Fatal("the fragmented connection did not deliver the payload")
	}
}

func TestNewFragmentWithOption(t *testing.T) {
	fragment, err := NewFragmentWithOption(FragmentOption{Name: "frag"})
	require.NoError(t, err)
	require.Equal(t, C.Fragment, fragment.Type())
	require.True(t, fragment.SupportUDP())
	require.Equal(t, fragmentModeTLS, fragment.cfg.mode)
	require.Equal(t, defaultFragmentSegments, fragment.cfg.segments)
	require.Equal(t, defaultFragmentDelay, fragment.cfg.delay)
	require.True(t, fragment.cfg.splitSNI)
	require.True(t, fragment.cfg.fallbackPlain)

	fragment, err = NewFragmentWithOption(FragmentOption{
		Name:     "frag",
		Mode:     fragmentModeHTTP,
		Segments: 9,
		Delay:    -1,
	})
	require.NoError(t, err)
	require.Equal(t, 4, fragment.cfg.segments)
	require.Zero(t, fragment.cfg.delay)
	require.False(t, fragment.cfg.fallbackPlain)

	_, err = NewFragmentWithOption(FragmentOption{Name: "frag", Mode: "quic"})
	require.ErrorContains(t, err, "unsupported mode")
}
