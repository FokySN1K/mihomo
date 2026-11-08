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

// recordPayloads walks the stream as tls records and answers their payloads.
func recordPayloads(t *testing.T, data []byte) [][]byte {
	t.Helper()
	var payloads [][]byte
	for len(data) > 0 {
		if len(data) < recordHeaderLen {
			t.Fatalf("%d stray bytes at the end: % x", len(data), data)
		}
		if data[0] != tlsHandshake || data[1] != tlsRecordVersion {
			t.Fatalf("a record starts with % x, want 16 03", data[:2])
		}
		size := int(data[3])<<8 | int(data[4])
		if size < 1 || len(data) < recordHeaderLen+size {
			t.Fatalf("a record announces %d bytes, %d left", size, len(data)-recordHeaderLen)
		}
		payloads = append(payloads, data[recordHeaderLen:recordHeaderLen+size])
		data = data[recordHeaderLen+size:]
	}
	return payloads
}

// clientHello builds a TLS handshake record of payload bytes.
func clientHello(payload int) []byte {
	record := []byte{0x16, 0x03, 0x01, byte(payload >> 8), byte(payload)}
	return append(record, bytes.Repeat([]byte{0x2a}, payload)...)
}

func TestClientHelloLeavesInOneWrite(t *testing.T) {
	raw := &fakeConn{}
	conn := NewConn(raw, &Config{Enabled: true, Length: "10-20", Delay: "0", Packets: "tlshello"})
	hello := clientHello(300)

	n, err := conn.Write(hello)
	if err != nil || n != len(hello) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(hello))
	}
	if len(raw.writes) != 1 {
		t.Fatalf("the handshake left in %d write(s), want 1", len(raw.writes))
	}
	payloads := recordPayloads(t, raw.writes[0])
	if len(payloads) < 2 {
		t.Fatalf("the handshake left in %d record(s), want it split", len(payloads))
	}
	var joined []byte
	for i, payload := range payloads {
		if len(payload) > 20 {
			t.Fatalf("record %d carries %d bytes, want at most 20", i, len(payload))
		}
		joined = append(joined, payload...)
	}
	if !bytes.Equal(joined, hello[recordHeaderLen:]) {
		t.Fatal("the records do not add up to the handshake")
	}
}

func TestDelayKeepsRecordsInSeparateWrites(t *testing.T) {
	raw := &fakeConn{}
	conn := NewConn(raw, &Config{Enabled: true, Length: "10-20", Delay: "1-2", Packets: "tlshello"})
	hello := clientHello(100)

	if _, err := conn.Write(hello); err != nil {
		t.Fatal(err)
	}
	if len(raw.writes) < 2 {
		t.Fatalf("the handshake left in %d write(s), want it split", len(raw.writes))
	}
	for i, write := range raw.writes {
		if len(write) > recordHeaderLen+20 {
			t.Fatalf("record %d is %d bytes, want at most %d", i, len(write), recordHeaderLen+20)
		}
	}
	joined := bytes.Join(recordPayloads(t, raw.flat()), nil)
	if !bytes.Equal(joined, hello[recordHeaderLen:]) {
		t.Fatal("the records do not add up to the handshake")
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

func TestTrafficAfterTheHandshakeInTheSameWriteLeavesAsIs(t *testing.T) {
	raw := &fakeConn{}
	conn := NewConn(raw, &Config{Enabled: true, Length: "10-20"})
	hello := clientHello(64)
	tail := []byte("more bytes of the same write")
	write := append(append([]byte(nil), hello...), tail...)

	if n, err := conn.Write(write); err != nil || n != len(write) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(write))
	}
	if len(raw.writes) != 1 {
		t.Fatalf("the write left in %d write(s), want 1", len(raw.writes))
	}
	stream := raw.writes[0]
	if !bytes.HasSuffix(stream, tail) {
		t.Fatalf("the stream does not end with % x", tail)
	}
	joined := bytes.Join(recordPayloads(t, stream[:len(stream)-len(tail)]), nil)
	if !bytes.Equal(joined, hello[recordHeaderLen:]) {
		t.Fatal("the records do not add up to the handshake")
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
	if len(raw.writes) != 1 {
		t.Fatalf("the handshake left in %d write(s), want 1", len(raw.writes))
	}
	joined := bytes.Join(recordPayloads(t, raw.flat()), nil)
	if !bytes.Equal(joined, hello[recordHeaderLen:]) {
		t.Fatal("the records do not add up to the handshake")
	}
}

func TestRecordThatDoesNotFitIsPassedThrough(t *testing.T) {
	raw := &fakeConn{}
	conn := NewConn(raw, &Config{Enabled: true, Length: "10-20"})
	announced := append([]byte{0x16, 0x03, 0x01, 0xff, 0xff}, bytes.Repeat([]byte{0x2a}, 64)...)

	n, err := conn.Write(announced)
	if err != nil || n != len(announced) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(announced))
	}
	if len(raw.writes) != 1 || !bytes.Equal(raw.writes[0], announced) {
		t.Fatalf("got %d write(s), want the first bytes as they came", len(raw.writes))
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
