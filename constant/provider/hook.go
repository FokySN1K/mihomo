package provider

import "sync"

// The initial load of a provider is the moment a group built from provider
// proxies can stop waiting for them. The loader reports it here, so such a
// group reacts to the load itself instead of polling the provider list.
var (
	loadedMu   sync.Mutex
	loadedSubs = make(map[uint64]func(name string))
	loadedNext uint64
)

// NotifyLoaded reports that the provider with the given name finished its
// initial load. Subscribers run in the goroutine of the loader, so they must
// not block it.
func NotifyLoaded(name string) {
	loadedMu.Lock()
	subs := make([]func(string), 0, len(loadedSubs))
	for _, sub := range loadedSubs {
		subs = append(subs, sub)
	}
	loadedMu.Unlock()

	for _, sub := range subs {
		sub(name)
	}
}

// OnLoaded subscribes to NotifyLoaded and returns the function that removes
// the subscription.
func OnLoaded(sub func(name string)) (cancel func()) {
	if sub == nil {
		return func() {}
	}

	loadedMu.Lock()
	loadedNext++
	id := loadedNext
	loadedSubs[id] = sub
	loadedMu.Unlock()

	return func() {
		loadedMu.Lock()
		delete(loadedSubs, id)
		loadedMu.Unlock()
	}
}
