package fragment

import (
	"errors"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/log"
)

const (
	// tlsHandshake is the content type of the ClientHello record.
	tlsHandshake    = 0x16
	recordHeaderLen = 5
	maxRecordLen    = 1 << 14

	defaultMinLength = 10
	defaultMaxLength = 20
)

// Config is the `fragment` proxy option. It mirrors the xray FinalMask tcp
// fragment settings so that a subscription shipping `fm` can be converted
// without losing the evasion it asked for.
type Config struct {
	Enabled bool   `proxy:"enabled,omitempty"`
	Length  string `proxy:"length,omitempty"`  // "10-20", a range in bytes
	Delay   string `proxy:"delay,omitempty"`   // "0", a range in milliseconds between pieces
	Packets string `proxy:"packets,omitempty"` // only "tlshello" is implemented
}

// Conn splits the first TLS record into pieces. A middlebox that parses the
// handshake out of one segment never sees the whole server name, while the peer
// reassembles it from the stream. Every write after that record, and every
// connection whose first write is not a TLS handshake, passes through as is.
type Conn struct {
	net.Conn

	minLength, maxLength int
	minDelay, maxDelay   time.Duration
	rng                  *rand.Rand

	mu      sync.Mutex
	started bool
	header  []byte // the record header when it arrives split across writes
	left    int    // bytes of the handshake record still to send
}

// NewConn wraps c, or answers c itself when fragmentation is off or the option
// cannot be honoured: a bad option must never break dialing.
func NewConn(c net.Conn, cfg *Config) net.Conn {
	if c == nil || cfg == nil || !cfg.Enabled {
		return c
	}
	if packets := strings.ToLower(strings.TrimSpace(cfg.Packets)); packets != "" && packets != "tlshello" {
		return c
	}
	minLength, maxLength, err := parseRange(cfg.Length, defaultMinLength, defaultMaxLength)
	if err != nil || minLength < 1 || maxLength < minLength {
		return c
	}
	minDelay, maxDelay, err := parseRange(cfg.Delay, 0, 0)
	if err != nil || minDelay < 0 || maxDelay < minDelay {
		return c
	}
	return &Conn{
		Conn:      c,
		minLength: minLength,
		maxLength: maxLength,
		minDelay:  time.Duration(minDelay) * time.Millisecond,
		maxDelay:  time.Duration(maxDelay) * time.Millisecond,
		rng:       rand.New(rand.NewSource(rand.Int63())),
	}
}

func (c *Conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	held := 0
	if !c.started {
		// The header can arrive split across writes; hold it until all five
		// bytes are here, the record length lives in the last two.
		if len(c.header)+len(p) < recordHeaderLen {
			c.header = append(c.header, p...)
			return len(p), nil
		}
		if len(c.header) > 0 {
			held = len(c.header)
			p = append(c.header, p...)
			c.header = nil
		}
		c.started = true
		c.left = handshakeLength(p)
	}

	written, pieces, err := c.writePieces(p)
	if pieces > 1 {
		log.Debugln("fragment: %d bytes of the handshake left in %d pieces", written-held, pieces)
	}
	if err != nil {
		return maxInt(written-held, 0), err
	}
	return written - held, nil
}

// writePieces sends the part of p that still belongs to the handshake record in
// pieces and everything after it in one write. It answers how many writes it took.
func (c *Conn) writePieces(p []byte) (int, int, error) {
	sent := 0
	pieces := 0
	for sent < len(p) {
		if c.left <= 0 {
			n, err := c.Conn.Write(p[sent:])
			return sent + n, pieces + 1, err
		}
		size := c.pieceSize(minInt(c.left, len(p)-sent))
		n, err := c.Conn.Write(p[sent : sent+size])
		sent += n
		pieces++
		c.left -= n
		if err != nil {
			return sent, pieces, err
		}
		if c.left > 0 && c.maxDelay > 0 {
			time.Sleep(c.pieceDelay())
		}
	}
	return sent, pieces, nil
}

func (c *Conn) pieceSize(limit int) int {
	if c.maxLength <= c.minLength {
		return minInt(c.minLength, limit)
	}
	return minInt(c.minLength+c.rng.Intn(c.maxLength-c.minLength+1), limit)
}

func (c *Conn) pieceDelay() time.Duration {
	if c.maxDelay <= c.minDelay {
		return c.minDelay
	}
	return c.minDelay + time.Duration(c.rng.Int63n(int64(c.maxDelay-c.minDelay+1)))
}

// handshakeLength answers the size of the first TLS record when p starts with a
// handshake record, and 0 when p is anything else or the length makes no sense.
func handshakeLength(p []byte) int {
	if len(p) < recordHeaderLen || p[0] != tlsHandshake || p[1] != 0x03 {
		return 0
	}
	length := int(p[3])<<8 | int(p[4])
	if length <= 0 || length > maxRecordLen {
		return 0
	}
	return recordHeaderLen + length
}

// parseRange reads "10", "10-20" or an empty string, which answers the defaults.
func parseRange(raw string, defaultMin, defaultMax int) (int, int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultMin, defaultMax, nil
	}
	low, high, found := strings.Cut(raw, "-")
	minValue, err := strconv.Atoi(strings.TrimSpace(low))
	if err != nil {
		return 0, 0, err
	}
	if !found {
		return minValue, minValue, nil
	}
	maxValue, err := strconv.Atoi(strings.TrimSpace(high))
	if err != nil {
		return 0, 0, err
	}
	if maxValue < minValue {
		return 0, 0, errors.New("fragment: the range ends before it starts")
	}
	return minValue, maxValue, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
