package outbound

import (
	"net"
	"strings"

	"github.com/metacubex/mihomo/component/fragment"
	"github.com/metacubex/mihomo/log"
)

// autoFragmentForReality enables handshake fragmentation for a REALITY proxy
// whose server name is a bare IP address.
//
// A middlebox that reads the SNI out of one TCP segment drops such a handshake,
// so the providers publish these nodes together with xray's `fm` option. The
// Clash profile generated for this core has no field for it, and the node that
// works in an xray client times out here. The preset matches what those links
// carry: split only the ClientHello, 10-20 bytes a piece, no delay.
//
// An explicit `fragment` option in the profile always wins: it is how a user
// can turn the splitting off (`enabled: false`) or tune it.
func autoFragmentForReality(name string, reality bool, serverName, server string) *fragment.Config {
	if !reality || !isIPServerName(serverName, server) {
		return nil
	}
	log.Debugln("fragment: %s uses REALITY with an IP server name, splitting the first record", name)
	return fragment.DefaultConfig()
}

// isIPServerName reports whether the name a TLS handshake would carry is a bare
// IP address. An empty server name falls back to the server address, which is
// what the TLS layer uses.
func isIPServerName(serverName, server string) bool {
	name := strings.TrimSpace(serverName)
	if name == "" {
		name = strings.TrimSpace(server)
	}
	name = strings.TrimSuffix(strings.TrimPrefix(name, "["), "]")
	return net.ParseIP(name) != nil
}
