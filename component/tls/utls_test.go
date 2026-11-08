package tls

import (
	"testing"

	utls "github.com/metacubex/utls"
)

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
