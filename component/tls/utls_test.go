package tls

import (
	"net"
	"testing"
	"time"

	utls "github.com/metacubex/utls"
	"golang.org/x/exp/slices"
)

// fakeConn is only there to hand UClient a net.Conn; building a handshake
// state never writes to it.
type fakeConn struct{}

func (c *fakeConn) Read([]byte) (int, error)         { return 0, nil }
func (c *fakeConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *fakeConn) Close() error                     { return nil }
func (c *fakeConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *fakeConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }

func TestSpecWithX25519MLKEM768(t *testing.T) {
	spec, err := SpecWithX25519MLKEM768(utls.HelloFirefox_Auto)
	if err != nil || spec == nil {
		t.Fatalf("firefox spec = %v, %v; want an upgraded spec", spec, err)
	}

	uConn := utls.UClient(&fakeConn{}, &utls.Config{ServerName: "example.org"}, utls.HelloCustom)
	if err := uConn.ApplyPreset(spec); err != nil {
		t.Fatalf("ApplyPreset: %v", err)
	}
	if err := uConn.BuildHandshakeState(); err != nil {
		t.Fatalf("BuildHandshakeState: %v", err)
	}

	var curves []utls.CurveID
	var shares []utls.KeyShare
	for _, extension := range uConn.Extensions {
		switch ext := extension.(type) {
		case *utls.SupportedCurvesExtension:
			curves = ext.Curves
		case *utls.KeyShareExtension:
			shares = ext.KeyShares
		}
	}
	if !slices.Contains(curves, utls.X25519MLKEM768) {
		t.Fatalf("supported groups %v do not offer X25519MLKEM768", curves)
	}
	if len(shares) == 0 || shares[0].Group != utls.X25519MLKEM768 {
		t.Fatalf("the first key share is not X25519MLKEM768: %+v", shares)
	}
	// ML-KEM-768 encapsulation key followed by the X25519 share.
	if len(shares[0].Data) != 1184+32 {
		t.Fatalf("the hybrid share carries %d bytes, want %d", len(shares[0].Data), 1184+32)
	}
	if length := len(uConn.HandshakeState.Hello.Raw); length < 1700 {
		t.Fatalf("the hello is %d bytes, want the ~1.8 KB of a current browser", length)
	}
}

func TestSpecWithX25519MLKEM768SkipsWhatAlreadyHasIt(t *testing.T) {
	for name, fingerprint := range map[string]UClientHelloID{
		"chrome":     utls.HelloChrome_Auto,
		"randomized": utls.HelloRandomized,
	} {
		spec, err := SpecWithX25519MLKEM768(fingerprint)
		if err != nil || spec != nil {
			t.Fatalf("%s spec = %v, %v; want nil, nil", name, spec, err)
		}
	}
}

func TestGetFingerprint(t *testing.T) {
	tests := []struct {
		value string
		want  UClientHelloID
		ok    bool
	}{
		{"firefox", utls.HelloFirefox_Auto, true},
		{"hellochrome_120", utls.HelloChrome_120, true},
		{"hellofirefox_120", utls.HelloFirefox_120, true},
		{"hellosafari_16_0", utls.HelloSafari_16_0, true},
		{"hellochrome", utls.HelloChrome_Auto, true},
		{"none", UClientHelloID{}, false},
		{"hellobogus", UClientHelloID{}, false},
	}

	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			fingerprint, ok := GetFingerprint(test.value)
			if ok != test.ok {
				t.Fatalf("ok = %v, want %v", ok, test.ok)
			}
			if ok && (fingerprint.Client != test.want.Client || fingerprint.Version != test.want.Version) {
				t.Fatalf("got %s %s, want %s %s", fingerprint.Client, fingerprint.Version, test.want.Client, test.want.Version)
			}
		})
	}
}
