package profile

import (
	"github.com/metacubex/mihomo/common/atomic"
)

// StoreSelected is a global switch for storing selected proxy to cache
var StoreSelected = atomic.NewBool(true)

// StoreReputation is a global switch for storing the reputation of the nodes
// in cache, so the faster ones are still known after a restart
var StoreReputation = atomic.NewBool(false)
