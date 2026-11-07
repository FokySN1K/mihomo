package fragment

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// fakeConn records every Write so the test can see how the handshake left.
type fakeConn struct {
	writes [][]byte
}

func (c *fakeConn) Read([]byte) (int, error)         { return 0, nil }
func (c *fakeConn) Close() error                     { return nil }
func (c *fakeConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *fakeConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }

func (c *fakeConn) Write(p []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (c *fakeConn) flat() []byte {
	var out []byte
	for _, write := range c.writes {
		out = append(out, write...)
	}
	return out
}

// clientHello builds a TLS handshake record of payload bytes.
func clientHello(payload int) []byte {
	record := []byte{0x16, 0x03, 0x01, byte(payload >> 8), byte(payload)}
	return append(record, bytes.Repeat([]byte{0x2a}, payload)...)
}

func TestClientHelloLeavesInPieces(t *testing.T) {
	raw := &fakeConn{}
	conn := NewConn(raw, &Config{Enabled: true, Length: "10-20", Delay: "0", Packets: "tlshello"})
	hello := clientHello(300)

	n, err := conn.Write(hello)
	if err != nil || n != len(hello) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(hello))
	}
	if len(raw.writes) < 2 {
		t.Fatalf("the handshake left in %d write(s), want it split", len(raw.writes))
	}
	for i, write := range raw.writes {
		if len(write) > 20 {
			t.Fatalf("piece %d is %d bytes, want at most 20", i, len(write))
		}
	}
	if !bytes.Equal(raw.flat(), hello) {
		t.Fatal("the pieces do not add up to the handshake")
	}
}

func TestTrafficAfterTheHandshakePassesThrough(t *testing.T) {
	raw := &fakeConn{}
	conn := NewConn(raw, &Config{Enabled: true, Length: "10-20", Packets: "tlshello"})
	if _, err := conn.Write(clientHello(64)); err != nil {
		t.Fatal(err)
	}
	request := []byte("GET / HTTP/1.1\r\nHost: example.org\r\n\r\n")
	before := len(raw.writes)
	n, err := conn.Write(request)
	if err != nil || n != len(request) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(request))
	}
	if len(raw.writes) != before+1 {
		t.Fatalf("the request took %d writes, want 1", len(raw.writes)-before)
	}
	if !bytes.Equal(raw.writes[len(raw.writes)-1], request) {
		t.Fatal("the request did not pass through unchanged")
	}
}

func TestFirstWriteThatIsNotTLSIsPassedThrough(t *testing.T) {
	raw := &fakeConn{}
	conn := NewConn(raw, &Config{Enabled: true, Length: "10-20"})
	payload := []byte("plain application bytes, no handshake here")
	n, err := conn.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(payload))
	}
	if len(raw.writes) != 1 || !bytes.Equal(raw.writes[0], payload) {
		t.Fatalf("got %d write(s), want the payload in one", len(raw.writes))
	}
}

func TestHeaderSplitAcrossWrites(t *testing.T) {
	raw := &fakeConn{}
	conn := NewConn(raw, &Config{Enabled: true, Length: "10-20"})
	hello := clientHello(100)

	if n, err := conn.Write(hello[:2]); err != nil || n != 2 {
		t.Fatalf("first Write = %d, %v; want 2, nil", n, err)
	}
	if len(raw.writes) != 0 {
		t.Fatal("the header left before the record length arrived")
	}
	if n, err := conn.Write(hello[2:]); err != nil || n != len(hello)-2 {
		t.Fatalf("second Write = %d, %v; want %d, nil", n, err, len(hello)-2)
	}
	if !bytes.Equal(raw.flat(), hello) {
		t.Fatal("the pieces do not add up to the handshake")
	}
	for i, write := range raw.writes {
		if len(write) > 20 {
			t.Fatalf("piece %d is %d bytes, want at most 20", i, len(write))
		}
	}
}

func TestOptionsThatCannotBeHonouredAnswerTheSameConn(t *testing.T) {
	raw := &fakeConn{}
	cases := []*Config{
		nil,
		{},
		{Enabled: true, Packets: "1-3"},
		{Enabled: true, Length: "20-10"},
		{Enabled: true, Length: "abc"},
		{Enabled: true, Delay: "5-1"},
	}
	for _, cfg := range cases {
		if got := NewConn(raw, cfg); got != net.Conn(raw) {
			t.Fatalf("NewConn(%+v) wrapped the conn, want it as is", cfg)
		}
	}
}

func TestEmptyWrite(t *testing.T) {
	raw := &fakeConn{}
	conn := NewConn(raw, &Config{Enabled: true})
	if n, err := conn.Write(nil); err != nil || n != 0 {
		t.Fatalf("Write(nil) = %d, %v; want 0, nil", n, err)
	}
	if len(raw.writes) != 0 {
		t.Fatal("an empty write reached the conn")
	}
}
