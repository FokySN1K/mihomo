package fragment

import (
	"errors"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// tlsHandshake is the content type of the ClientHello record.
	tlsHandshake     = 0x16
	tlsRecordVersion = 0x03
	recordHeaderLen  = 5
	maxRecordLen     = 1 << 14

	defaultMinLength = 10
	defaultMaxLength = 20
)

// The zero value is the init state, so the first write always goes through the
// header check.
const (
	stateInit = iota
	stateHandshake
	stateBypass
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

// Conn rewrites the first TLS record into a run of small records of its own.
// A middlebox that parses one record to find the server name never sees the
// whole handshake, while the peer reassembles it from the stream: TLS allows a
// handshake message to span records, so this is what xray does for `fm`.
// The run leaves in a single write, the way FinalMask sends it: the DPI on
// this path drops a hello spread over many small TCP segments even when every
// record is small. A configured delay needs the writes apart and is honoured
// one record at a time.
// Every write after that record, and every connection whose first write is not
// a TLS handshake, passes through as is.
type Conn struct {
	net.Conn

	minLength, maxLength int
	minDelay, maxDelay   time.Duration
	rng                  *rand.Rand

	mu     sync.Mutex
	state  int
	held   []byte // the first bytes of the connection until the record is complete
	needed int    // size of the record, header included, once the header is known
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

	if c.state == stateBypass {
		return c.Conn.Write(p)
	}

	// Every piece becomes a record of its own, so the whole handshake record
	// has to be here before the first piece leaves.
	c.held = append(c.held, p...)

	if c.state == stateInit {
		// The header can arrive split across writes, the record length lives
		// in the last two bytes.
		if len(c.held) < recordHeaderLen {
			return len(p), nil
		}
		length := int(c.held[3])<<8 | int(c.held[4])
		if c.held[0] != tlsHandshake || c.held[1] != tlsRecordVersion || length <= 0 || length > maxRecordLen {
			return c.passThrough(len(p))
		}
		c.needed = recordHeaderLen + length
		c.state = stateHandshake
	}

	if len(c.held) < c.needed {
		return len(p), nil
	}
	return c.writeRecords(len(p))
}

// passThrough forwards everything held as it came and stops interfering.
func (c *Conn) passThrough(n int) (int, error) {
	held := c.held
	c.held = nil
	c.state = stateBypass
	if _, err := c.Conn.Write(held); err != nil {
		return 0, err
	}
	return n, nil
}

// writeRecords sends the handshake as records of at most maxLength bytes in
// their payload, then whatever followed that record in the same write, and
// answers n for the write the caller made.
func (c *Conn) writeRecords(n int) (int, error) {
	payload := c.held[recordHeaderLen:c.needed]
	tail := c.held[c.needed:]
	versionHigh, versionLow := c.held[1], c.held[2]

	c.held = nil
	c.state = stateBypass

	record := make([]byte, recordHeaderLen+c.maxLength)
	record[0], record[1], record[2] = tlsHandshake, versionHigh, versionLow

	// Without a delay the whole run goes in one write. A middlebox on this
	// path drops the hello when the records trickle out as separate
	// segments, and only the split into small records matters to TLS, not
	// the number of writes.
	if c.maxDelay == 0 {
		out := make([]byte, 0, len(payload)+len(tail)+recordHeaderLen*(len(payload)/c.minLength+1))
		for sent := 0; sent < len(payload); {
			size := c.pieceSize(len(payload) - sent)
			record[3], record[4] = byte(size>>8), byte(size)
			copy(record[recordHeaderLen:recordHeaderLen+size], payload[sent:sent+size])
			out = append(out, record[:recordHeaderLen+size]...)
			sent += size
		}
		out = append(out, tail...)
		if _, err := c.Conn.Write(out); err != nil {
			return 0, err
		}
		return n, nil
	}

	for sent := 0; sent < len(payload); {
		size := c.pieceSize(len(payload) - sent)
		record[3], record[4] = byte(size>>8), byte(size)
		copy(record[recordHeaderLen:recordHeaderLen+size], payload[sent:sent+size])
		if _, err := c.Conn.Write(record[:recordHeaderLen+size]); err != nil {
			return 0, err
		}
		sent += size
		if sent < len(payload) {
			time.Sleep(c.pieceDelay())
		}
	}
	if len(tail) > 0 {
		if _, err := c.Conn.Write(tail); err != nil {
			return 0, err
		}
	}
	return n, nil
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
