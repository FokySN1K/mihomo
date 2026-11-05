package provider

import (
	"sync"

	"github.com/metacubex/mihomo/log"
)

// A provider installs its proxy list after the config has been applied, and
// replaces that list later when the subscription refreshes. A group that
// builds itself from provider proxies, and the tunnel's direct-name snapshot,
// both have to react to that, so a provider reports it here instead of being
// polled.
var (
	changedMu   sync.Mutex
	changedSubs = make(map[uint64]func(name string))
	changedNext uint64
)

// NotifyChanged reports that the provider with the given name installed a new
// proxy list. Subscribers run in the goroutine of the provider, so they must
// not block it.
func NotifyChanged(name string) {
	changedMu.Lock()
	subs := make([]func(string), 0, len(changedSubs))
	for _, sub := range changedSubs {
		subs = append(subs, sub)
	}
	changedMu.Unlock()

	for _, sub := range subs {
		notifyChanged(sub, name)
	}
}

// notifyChanged runs one subscriber. A subscriber that panics must neither
// take down the provider that loaded nor hide the change from the others.
func notifyChanged(sub func(string), name string) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorln("provider %s: subscriber panicked: %v", name, r)
		}
	}()
	sub(name)
}

// OnChange subscribes to NotifyChanged and returns the function that removes
// the subscription.
func OnChange(sub func(name string)) (cancel func()) {
	if sub == nil {
		return func() {}
	}

	changedMu.Lock()
	changedNext++
	id := changedNext
	changedSubs[id] = sub
	changedMu.Unlock()

	return func() {
		changedMu.Lock()
		delete(changedSubs, id)
		changedMu.Unlock()
	}
}
