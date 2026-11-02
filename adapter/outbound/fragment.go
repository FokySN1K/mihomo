package outbound

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/loopback"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
)

const (
	fragmentModeTLS  = "tls"
	fragmentModeHTTP = "http"
	fragmentModeAny  = "any"

	defaultFragmentSplitPosition = 3
	defaultFragmentSegments      = 2
	defaultFragmentDelay         = 10 * time.Millisecond
	defaultFragmentFallback      = 5 * time.Second
	maxFragmentDelay             = time.Second
)

type FragmentOption struct {
	BasicOption
	Name string `proxy:"name"`
	// Mode decides when the first write is segmented: "tls" only splits a
	// TLS ClientHello, "http" and "any" split the first write of any protocol.
	Mode string `proxy:"mode,omitempty"`
	// SplitPosition is the byte offset of the split when the ClientHello
	// can't be parsed (or mode isn't tls). Zero uses the default.
	SplitPosition int `proxy:"split-position,omitempty"`
	// SplitSNI splits in the middle of the SNI host name when the first write
	// contains a parseable TLS ClientHello.
	SplitSNI *bool `proxy:"split-sni,omitempty"`
	// Segments is how many pieces the first write is cut into: 2, 3 or 4.
	Segments int `proxy:"segments,omitempty"`
	// Delay is the pause between the segments in milliseconds.
	Delay int `proxy:"delay,omitempty"`
	// TCPNoDelay disables Nagle on the dialed socket so the segments leave as
	// separate packets more often.
	TCPNoDelay *bool `proxy:"tcp-no-delay,omitempty"`
	// FallbackPlain retries the first write without fragmentation when the
	// fragmented write fails. It is on by default for the tls mode.
	FallbackPlain *bool `proxy:"fallback-plain,omitempty"`
}

type fragmentConfig struct {
	mode            string
	splitPosition   int
	splitSNI        bool
	segments        int
	delay           time.Duration
	tcpNoDelay      bool
	fallbackPlain   bool
	fallbackTimeout time.Duration
}

// Fragment is a DIRECT-like outbound that cuts the first write of a
// connection into several TCP segments. Without root it cannot craft packets,
// so segmentation is the only DPI-bypass primitive available to it; it is
// meant to be used directly or as a candidate of cascade/url-test-pool.
type Fragment struct {
	*Base
	loopBack *loopback.Detector
	cfg      fragmentConfig
}

// DialContext implements C.ProxyAdapter
func (f *Fragment) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if err := f.loopBack.CheckConn(metadata); err != nil {
		return nil, err
	}

	opts := f.DialOptions()
	opts = append(opts, dialer.WithResolver(resolver.DirectHostResolver))
	dial := func(dialCtx context.Context) (net.Conn, error) {
		return dialer.DialContext(resolver.WithInitiator(dialCtx, resolver.InitiatorDirect), "tcp", metadata.RemoteAddress(), opts...)
	}

	raw, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	if f.cfg.tcpNoDelay {
		if tcp, ok := raw.(interface{ SetNoDelay(bool) error }); ok {
			_ = tcp.SetNoDelay(true)
		}
	}

	fc := &fragmentConn{
		Conn: raw,
		cfg:  f.cfg,
		dial: dial,
	}
	return f.loopBack.NewConn(NewConn(fc, f)), nil
}

// ListenPacketContext implements C.ProxyAdapter. UDP is passed through: a
// datagram cannot be split at the application level without raw sockets.
func (f *Fragment) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	if err := f.loopBack.CheckPacketConn(metadata); err != nil {
		return nil, err
	}
	if err := f.ResolveUDP(ctx, metadata); err != nil {
		return nil, err
	}
	pc, err := dialer.NewDialer(f.DialOptions()...).ListenPacket(ctx, "udp", "", metadata.AddrPort())
	if err != nil {
		return nil, err
	}
	return f.loopBack.NewPacketConn(NewPacketConn(pc, f)), nil
}

// ResolveUDP implements ProxyAdapter
func (f *Fragment) ResolveUDP(ctx context.Context, metadata *C.Metadata) error {
	if (!metadata.Resolved() || resolver.DirectHostResolver != resolver.DefaultResolver) && metadata.Host != "" {
		ip, err := resolveIPWithResolver(resolver.WithInitiator(ctx, resolver.InitiatorDirect), metadata.Host, f.prefer, resolver.DirectHostResolver)
		if err != nil {
			return fmt.Errorf("can't resolve ip: %w", err)
		}
		metadata.DstIP = ip
	}
	return nil
}

// IsL3Protocol implements C.ProxyAdapter
func (f *Fragment) IsL3Protocol(metadata *C.Metadata) bool {
	return true
}

// MarshalJSON implements C.ProxyAdapter
func (f *Fragment) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"type":          C.Fragment.String(),
		"id":            f.Id(),
		"mode":          f.cfg.mode,
		"splitPosition": f.cfg.splitPosition,
		"splitSNI":      f.cfg.splitSNI,
		"segments":      f.cfg.segments,
		"delay":         f.cfg.delay.Milliseconds(),
		"fallbackPlain": f.cfg.fallbackPlain,
	})
}

func NewFragmentWithOption(option FragmentOption) (*Fragment, error) {
	mode := option.Mode
	if mode == "" {
		mode = fragmentModeTLS
	}
	switch mode {
	case fragmentModeTLS, fragmentModeHTTP, fragmentModeAny:
	default:
		return nil, fmt.Errorf("fragment: unsupported mode %q", option.Mode)
	}

	splitPosition := option.SplitPosition
	if splitPosition <= 0 {
		splitPosition = defaultFragmentSplitPosition
	}

	splitSNI := true
	if option.SplitSNI != nil {
		splitSNI = *option.SplitSNI
	}

	segments := option.Segments
	if segments <= 0 {
		segments = defaultFragmentSegments
	}
	if segments < 2 {
		segments = 2
	}
	if segments > 4 {
		segments = 4
	}

	delay := defaultFragmentDelay
	if option.Delay != 0 {
		delay = time.Duration(option.Delay) * time.Millisecond
	}
	if delay < 0 {
		delay = 0
	}
	if delay > maxFragmentDelay {
		delay = maxFragmentDelay
	}

	tcpNoDelay := true
	if option.TCPNoDelay != nil {
		tcpNoDelay = *option.TCPNoDelay
	}

	fallbackPlain := mode == fragmentModeTLS
	if option.FallbackPlain != nil {
		fallbackPlain = *option.FallbackPlain
	}

	return &Fragment{
		Base: NewBase(BaseOption{
			Name:         option.Name,
			Type:         C.Fragment,
			ProviderName: option.ProviderName,
			UDP:          true,
			TFO:          option.TFO,
			MPTCP:        option.MPTCP,
			Interface:    option.Interface,
			RoutingMark:  option.RoutingMark,
			Prefer:       option.IPVersion,
		}),
		loopBack: loopback.NewDetector(),
		cfg: fragmentConfig{
			mode:            mode,
			splitPosition:   splitPosition,
			splitSNI:        splitSNI,
			segments:        segments,
			delay:           delay,
			tcpNoDelay:      tcpNoDelay,
			fallbackPlain:   fallbackPlain,
			fallbackTimeout: defaultFragmentFallback,
		},
	}, nil
}

// fragmentConn cuts the first write into several TCP segments. Everything
// after that is passed through unchanged.
type fragmentConn struct {
	net.Conn

	cfg  fragmentConfig
	dial func(context.Context) (net.Conn, error)

	mu            sync.Mutex
	done          bool
	fallbackTried bool
}

func (c *fragmentConn) current() net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn
}

func (c *fragmentConn) replace(conn net.Conn) net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.Conn
	c.Conn = conn
	return old
}

func (c *fragmentConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.done {
		c.mu.Unlock()
		return c.current().Write(p)
	}
	c.done = true
	c.mu.Unlock()

	n, err := c.writeFragmented(p)
	if err == nil {
		return n, nil
	}

	if c.cfg.fallbackPlain {
		c.mu.Lock()
		tried := c.fallbackTried
		c.fallbackTried = true
		c.mu.Unlock()
		if !tried {
			if fn, ferr := c.replayPlain(p); ferr == nil {
				return fn, nil
			}
		}
	}
	return n, err
}

func (c *fragmentConn) writeFragmented(p []byte) (int, error) {
	if !c.shouldFragment(p) {
		return writeFull(c.current(), p)
	}

	offsets := c.splitOffsets(p)
	if len(offsets) == 0 {
		return writeFull(c.current(), p)
	}

	conn := c.current()
	start := 0
	for _, off := range offsets {
		n, err := writeFull(conn, p[start:off])
		if err != nil {
			return start + n, err
		}
		start = off
		if c.cfg.delay > 0 {
			time.Sleep(c.cfg.delay)
		}
	}

	n, err := writeFull(conn, p[start:])
	return start + n, err
}

func (c *fragmentConn) shouldFragment(p []byte) bool {
	switch c.cfg.mode {
	case fragmentModeHTTP, fragmentModeAny:
		return len(p) > 1
	default: // tls
		return len(p) >= 3 && p[0] == 0x16 && p[1] == 0x03
	}
}

func (c *fragmentConn) splitOffsets(p []byte) []int {
	if len(p) < 2 {
		return nil
	}

	pos := 0
	if c.cfg.splitSNI {
		if off, ok := tlsClientHelloSNIOffset(p); ok {
			pos = off
		}
	}
	if pos <= 0 {
		pos = c.cfg.splitPosition
	}
	if pos <= 0 {
		pos = defaultFragmentSplitPosition
	}
	if pos >= len(p) {
		pos = len(p) / 2
	}
	if pos < 1 || pos >= len(p) {
		return nil
	}

	switch c.cfg.segments {
	case 3:
		if pos > 1 {
			return []int{1, pos}
		}
	case 4:
		if pos > 2 {
			return []int{1, 2, pos}
		}
	}
	return []int{pos}
}

// replayPlain redials the destination and sends the buffered first write
// without segmentation. It is only reached after a failed fragmented write,
// before the peer has seen any response, so replaying a ClientHello is safe.
func (c *fragmentConn) replayPlain(p []byte) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.fallbackTimeout)
	defer cancel()

	conn, err := c.dial(ctx)
	if err != nil {
		return 0, err
	}
	if c.cfg.tcpNoDelay {
		if tcp, ok := conn.(interface{ SetNoDelay(bool) error }); ok {
			_ = tcp.SetNoDelay(true)
		}
	}
	if _, err = writeFull(conn, p); err != nil {
		_ = conn.Close()
		return 0, err
	}

	old := c.replace(conn)
	if old != nil {
		_ = old.Close()
	}
	return len(p), nil
}

func (c *fragmentConn) Read(p []byte) (int, error) {
	return c.current().Read(p)
}

func (c *fragmentConn) Close() error {
	return c.current().Close()
}

func (c *fragmentConn) SetDeadline(t time.Time) error {
	return c.current().SetDeadline(t)
}

func (c *fragmentConn) SetReadDeadline(t time.Time) error {
	return c.current().SetReadDeadline(t)
}

func (c *fragmentConn) SetWriteDeadline(t time.Time) error {
	return c.current().SetWriteDeadline(t)
}

func writeFull(w io.Writer, p []byte) (int, error) {
	written := 0
	for written < len(p) {
		n, err := w.Write(p[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrUnexpectedEOF
		}
	}
	return written, nil
}

// tlsClientHelloSNIOffset returns the offset in the middle of the SNI host
// name of a TLS ClientHello, or false when the buffer doesn't contain a
// complete, parseable ClientHello.
func tlsClientHelloSNIOffset(b []byte) (int, bool) {
	if len(b) < 6 || b[0] != 0x16 || b[1] != 0x03 || b[5] != 0x01 {
		return 0, false
	}

	p := 5 + 4 // record header + handshake header
	if p+2+32 > len(b) {
		return 0, false
	}
	p += 2 + 32 // legacy version + random

	if p >= len(b) {
		return 0, false
	}
	sessionLen := int(b[p])
	p++
	if p+sessionLen+2 > len(b) {
		return 0, false
	}
	p += sessionLen

	cipherLen := int(binary.BigEndian.Uint16(b[p:]))
	p += 2
	if p+cipherLen+1 > len(b) {
		return 0, false
	}
	p += cipherLen

	compLen := int(b[p])
	p++
	if p+compLen+2 > len(b) {
		return 0, false
	}
	p += compLen

	extLen := int(binary.BigEndian.Uint16(b[p:]))
	p += 2
	end := p + extLen
	if end > len(b) {
		end = len(b)
	}

	for p+4 <= end {
		extType := binary.BigEndian.Uint16(b[p:])
		extSize := int(binary.BigEndian.Uint16(b[p+2:]))
		p += 4
		if p+extSize > end {
			return 0, false
		}
		if extType != 0 { // server_name
			p += extSize
			continue
		}

		q := p
		if q+2 > end {
			return 0, false
		}
		q += 2 // server name list length
		if q+1 > end {
			return 0, false
		}
		if b[q] != 0 { // host_name
			return 0, false
		}
		q++
		if q+2 > end {
			return 0, false
		}
		nameLen := int(binary.BigEndian.Uint16(b[q:]))
		q += 2
		if nameLen == 0 || q+nameLen > end {
			return 0, false
		}
		return q + nameLen/2, true
	}
	return 0, false
}
