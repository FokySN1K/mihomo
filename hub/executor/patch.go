package executor

import "sync/atomic"

type ProviderLoadedHook func(providerName string)

var DefaultProviderLoadedHook ProviderLoadedHook

// disableDefaultListeners is set by a host that opens every inbound itself and
// keeps the ports down until the user starts the service, which is what the
// FlClash core does.
var disableDefaultListeners atomic.Bool

// DisableDefaultListeners stops ApplyConfig from creating the inbound
// listeners. A standalone build never calls it and keeps binding mixed-port
// and the rest from ApplyConfig.
func DisableDefaultListeners() {
	disableDefaultListeners.Store(true)
}
