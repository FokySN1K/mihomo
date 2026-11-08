package tls

import (
	"errors"
	"testing"

	utls "github.com/metacubex/utls"
	"golang.org/x/exp/slices"
)

func TestRealityHelloFallsBackToTheRecordedPreset(t *testing.T) {
	fingerprint := utls.HelloFirefox_Auto
	helloFallbackReported.Delete(fingerprintName(fingerprint))
	previousSpec := specWithX25519MLKEM768
	specWithX25519MLKEM768 = func(UClientHelloID) (*utls.ClientHelloSpec, error) {
		return nil, errors.New("cannot convert")
	}
	t.Cleanup(func() { specWithX25519MLKEM768 = previousSpec })
	reports := 0
	DefaultHelloFallbackNotify = func(string, string) { reports++ }
	t.Cleanup(func() { DefaultHelloFallbackNotify = nil })

	uConn := realityHello(&fakeConn{}, &utls.Config{ServerName: "example.org"}, fingerprint, true)
	if err := uConn.BuildHandshakeState(); err != nil {
		t.Fatalf("the fallback hello of the recorded preset does not build: %v", err)
	}
	for _, extension := range uConn.Extensions {
		if curves, ok := extension.(*utls.SupportedCurvesExtension); ok && slices.Contains(curves.Curves, utls.X25519MLKEM768) {
			t.Fatal("the fallback hello still offers X25519MLKEM768")
		}
	}
	if reports != 1 {
		t.Fatalf("reported %d times, want the fallback reported once", reports)
	}
}

func TestReportHelloFallbackReportsOncePerFingerprint(t *testing.T) {
	fingerprint := utls.HelloChrome_120
	helloFallbackReported.Delete(fingerprintName(fingerprint))
	var reports []string
	DefaultHelloFallbackNotify = func(fingerprint string, reason string) {
		reports = append(reports, fingerprint+"|"+reason)
	}
	t.Cleanup(func() { DefaultHelloFallbackNotify = nil })

	reportHelloFallback(fingerprint, errors.New("boom"))
	reportHelloFallback(fingerprint, errors.New("boom"))

	want := fingerprintName(fingerprint) + "|boom"
	if len(reports) != 1 || reports[0] != want {
		t.Fatalf("reports = %v, want exactly %q", reports, want)
	}
}
