package config

import "sync"

var (
	proxyNameList   []string
	proxyNameListMu sync.RWMutex
)

// GetProxyNameList returns the names of the proxies the last apply produced.
// The caller gets its own copy: the list is rebuilt on every apply, and a
// caller that appends to the shared slice would corrupt the next one.
func GetProxyNameList() []string {
	proxyNameListMu.RLock()
	defer proxyNameListMu.RUnlock()
	return append([]string(nil), proxyNameList...)
}

func SetProxyNameList(list []string) {
	proxyNameListMu.Lock()
	defer proxyNameListMu.Unlock()
	proxyNameList = list
}
