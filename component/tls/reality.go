package tls

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/ntp"

	"github.com/metacubex/http"
	"github.com/metacubex/randv2"
	utls "github.com/metacubex/utls"
	"golang.org/x/crypto/hkdf"
)

const RealityMaxShortIDLen = 8

// The REALITY session id starts with the client version. Xray-core v26.7.11+
// servers reject clients that report a version below minClientVer (26.3.27 by
// default) and silently fall back to the mask site, which surfaces here as a
// certificate verification error. We therefore report a recent Xray-core
// version instead of the 1.8.2 marker that older servers ignored.
const (
	realityClientVersionMajor byte = 26
	realityClientVersionMinor byte = 10
	realityClientVersionPatch byte = 31
)

type RealityConfig struct {
	PublicKey *ecdh.PublicKey
	ShortID   [RealityMaxShortIDLen]byte

	SupportX25519MLKEM768 bool
}

func GetRealityConn(ctx context.Context, conn net.Conn, fingerprint UClientHelloID, serverName string, realityConfig *RealityConfig) (net.Conn, error) {
	for retry := 0; ; retry++ {
		verifier := &realityVerifier{
			serverName: serverName,
		}
		uConfig := &utls.Config{
			Time:                   ntp.Now,
			ServerName:             serverName,
			InsecureSkipVerify:     true,
			SessionTicketsDisabled: true,
			VerifyConnection:       verifier.VerifyConnection,
		}

		uConn := realityHello(conn, uConfig, fingerprint, realityConfig.SupportX25519MLKEM768)
		verifier.UConn = uConn
		err := uConn.BuildHandshakeState()
		if err != nil {
			return nil, err
		}

		if !realityConfig.SupportX25519MLKEM768 { // for X25519MLKEM768 does not work properly with the old reality server
			err = BuildRemovedX25519MLKEM768HandshakeState(uConn)
			if err != nil {
				return nil, err
			}
		}

		hello := uConn.HandshakeState.Hello
		rawSessionID := hello.Raw[39 : 39+32] // the location of session ID
		for i := range rawSessionID {         // https://github.com/golang/go/issues/5373
			rawSessionID[i] = 0
		}

		binary.BigEndian.PutUint64(hello.SessionId, uint64(ntp.Now().Unix()))

		copy(hello.SessionId[8:], realityConfig.ShortID[:])
		hello.SessionId[0] = realityClientVersionMajor
		hello.SessionId[1] = realityClientVersionMinor
		hello.SessionId[2] = realityClientVersionPatch

		//log.Debugln("REALITY hello.sessionId[:16]: %v", hello.SessionId[:16])

		keyShareKeys := uConn.HandshakeState.State13.KeyShareKeys
		if keyShareKeys == nil {
			// WTF???
			if retry > 2 {
				return nil, errors.New("nil keyShareKeys")
			}
			continue // retry
		}
		ecdheKey := keyShareKeys.Ecdhe
		if ecdheKey == nil {
			ecdheKey = keyShareKeys.MlkemEcdhe
		}
		if ecdheKey == nil {
			// WTF???
			if retry > 2 {
				return nil, errors.New("nil ecdheKey")
			}
			continue // retry
		}
		authKey, err := ecdheKey.ECDH(realityConfig.PublicKey)
		if err != nil {
			return nil, err
		}
		if authKey == nil {
			return nil, errors.New("nil auth_key")
		}
		verifier.authKey = authKey
		_, err = hkdf.New(sha256.New, authKey, hello.Random[:20], []byte("REALITY")).Read(authKey)
		if err != nil {
			return nil, err
		}
		aesBlock, _ := aes.NewCipher(authKey)
		aeadCipher, _ := cipher.NewGCM(aesBlock)
		aeadCipher.Seal(hello.SessionId[:0], hello.Random[20:], hello.SessionId[:16], hello.Raw)
		copy(hello.Raw[39:], hello.SessionId)
		//log.Debugln("REALITY hello.sessionId: %v", hello.SessionId)
		//log.Debugln("REALITY uConn.AuthKey: %v", authKey)

		err = uConn.HandshakeContext(ctx)
		if err != nil {
			return nil, err
		}

		log.Debugln("REALITY Authentication: %v, AEAD: %T", verifier.verified, aeadCipher)

		if !verifier.verified {
			go realityClientFallback(uConn, uConfig.ServerName, fingerprint)
			return nil, errors.New("REALITY authentication failed")
		}

		return uConn, nil
	}
}

// DefaultHelloFallbackNotify reports that the X25519MLKEM768 upgrade of a
// preset could not be built and the connection left with the hello as
// recorded. The core calls it at most once per fingerprint: the upgrade is
// deterministic, so a repeat would only repeat the same report. FlClash
// turns it into a notification, other clients see the log alone.
var DefaultHelloFallbackNotify func(fingerprint string, reason string)

// specWithX25519MLKEM768 is swapped by tests that need the upgrade to fail.
var specWithX25519MLKEM768 = SpecWithX25519MLKEM768

var helloFallbackReported sync.Map

// realityHello builds the client hello, adding the post-quantum key share to
// a preset that predates the browsers' ML-KEM support. Presets recorded before
// that carry no share, and their hello of 500-600 bytes is what the DPI on a
// mobile network drops first. An upgrade that cannot be built is not fatal:
// the preset as recorded is still a valid hello, so the dial keeps it and the
// failure is reported.
func realityHello(conn net.Conn, uConfig *utls.Config, fingerprint UClientHelloID, supportX25519MLKEM768 bool) *UConn {
	uConn := utls.UClient(conn, uConfig, fingerprint)
	if !supportX25519MLKEM768 {
		return uConn
	}
	spec, err := specWithX25519MLKEM768(fingerprint)
	if err != nil {
		reportHelloFallback(fingerprint, err)
		return uConn
	}
	if spec == nil {
		return uConn
	}
	custom := utls.UClient(conn, uConfig, utls.HelloCustom)
	if err := custom.ApplyPreset(spec); err != nil {
		reportHelloFallback(fingerprint, err)
		return uConn
	}
	return custom
}

func reportHelloFallback(fingerprint UClientHelloID, err error) {
	name := fingerprintName(fingerprint)
	// Every dial of a node with this preset repeats the failure, so only
	// the first one is worth a warning and a report.
	log.Debugln("TLS: cannot add X25519MLKEM768 to the hello of %s: %v", name, err)
	if _, reported := helloFallbackReported.LoadOrStore(name, struct{}{}); reported {
		return
	}
	log.Warnln("TLS: cannot add X25519MLKEM768 to the hello of %s, the original fingerprint is used: %v", name, err)
	if notify := DefaultHelloFallbackNotify; notify != nil {
		notify(name, err.Error())
	}
}

// fingerprintName spells a preset the way a profile does: chrome, chrome120.
// The Client field alone cannot tell the presets apart.
func fingerprintName(fingerprint UClientHelloID) string {
	return strings.ToLower(fingerprint.Client) + fingerprint.Version
}

func realityClientFallback(uConn net.Conn, serverName string, fingerprint utls.ClientHelloID) {
	defer uConn.Close()
	// use h2c mode to disallow the net/http fallback to http1.1
	//
	// Note that this usage is only applicable to our own net/http fork.
	// The standard library also needs to mask the tls.Conn type for the conn returned by DialTLSContext
	// see: https://github.com/golang/go/issues/79293#issuecomment-4426393534
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	client := http.Client{
		Transport: &http.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return uConn, nil
			},
			Protocols: protocols,
		},
	}
	request, err := http.NewRequest("GET", "https://"+serverName, nil)
	if err != nil {
		return
	}
	request.Header.Set("User-Agent", fingerprint.Client)
	request.AddCookie(&http.Cookie{Name: "padding", Value: strings.Repeat("0", randv2.IntN(32)+30)})
	response, err := client.Do(request)
	if err != nil {
		return
	}
	//_, _ = io.Copy(io.Discard, response.Body)
	time.Sleep(time.Duration(5+randv2.IntN(10)) * time.Second)
	response.Body.Close()
	client.CloseIdleConnections()
}

type realityVerifier struct {
	*utls.UConn
	serverName string
	authKey    []byte
	verified   bool
}

func (c *realityVerifier) VerifyConnection(state utls.ConnectionState) error {
	log.Debugln("REALITY localAddr: %v is using X25519MLKEM768 for TLS' communication: %v", c.RemoteAddr(), c.HandshakeState.ServerHello.ServerShare.Group == utls.X25519MLKEM768)
	certs := state.PeerCertificates
	if pub, ok := certs[0].PublicKey.(ed25519.PublicKey); ok {
		h := hmac.New(sha512.New, c.authKey)
		h.Write(pub)
		if bytes.Equal(h.Sum(nil), certs[0].Signature) {
			c.verified = true
			return nil
		}
	}
	opts := x509.VerifyOptions{
		DNSName:       c.serverName,
		Intermediates: x509.NewCertPool(),
		CurrentTime:   ntp.Now(),
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := certs[0].Verify(opts); err != nil {
		return err
	}
	return nil
}
