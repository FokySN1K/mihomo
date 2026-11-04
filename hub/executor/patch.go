package executor

import "sync/atomic"

type ProviderLoadedHook func(providerName string)

var DefaultProviderLoadedHook ProviderLoadedHook

// embedded is set by a host that runs the core inside its own process and owns
// the inbound lifecycle: ApplyConfig then leaves the ports down (the app starts
// the service itself) and returns without waiting for the initial provider
// load, because the app pulls the providers on its own schedule.
var embedded atomic.Bool

// DisableDefaultListeners marks this process as an embedded core. A standalone
// build never calls it and keeps binding mixed-port and the rest from
// ApplyConfig, and waits for the providers before it serves traffic.
func DisableDefaultListeners() {
	embedded.Store(true)
}
