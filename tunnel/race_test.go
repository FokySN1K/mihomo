package tunnel

import (
	"net/netip"
	"sync"
	"testing"

	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/trie"
	C "github.com/metacubex/mihomo/constant"
)

// TestResolveWhileApply guards the view a connection resolves against: an
// apply that replaces the proxies, the rules or a resolver on one goroutine
// must never be read half way by a dial on another. The test is about the
// race detector; a missed snapshot shows up as a torn read, not as a wrong
// answer.
func TestResolveWhileApply(t *testing.T) {
	direct := fakeProxy{name: "DIRECT", typ: C.Direct}
	UpdateProxies(map[string]C.Proxy{"DIRECT": direct}, nil)
	t.Cleanup(func() {
		UpdateProxies(nil, nil)
		UpdateRules(nil, nil, nil)
		SetMode(Rule)
	})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			UpdateProxies(map[string]C.Proxy{"DIRECT": direct}, nil)
			UpdateRules(nil, map[string][]C.Rule{"sub": nil}, nil)
			SetMode(Direct)
			SetMode(Rule)
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			UpdateSniffer(nil)
			SetSniffing(false)
			_ = IsSniffing()
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			hosts := resolver.NewHosts(trie.New[resolver.HostValue]())
			resolver.SetHosts(&hosts)
			resolver.DefaultResolver.Store(nil)
			resolver.SetHostMapper(nil)
			resolver.DisableIPv6.Store(true)
			resolver.UseSystemHosts.Store(false)
			resolver.SearchHosts("example.com", false)
			_ = resolver.MappingEnabled()
			_ = resolver.IsFakeIP(netip.MustParseAddr("198.18.0.1"))
			_ = resolver.DefaultResolver.Load()
		}
	}()

	for i := 0; i < 2000; i++ {
		metadata := &C.Metadata{Host: "example.com", NetWork: C.TCP, Type: C.SOCKS5}
		proxy, err := ResolveRoute(metadata)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if proxy == nil {
			t.Fatal("resolve picked no proxy")
		}
	}

	close(stop)
	wg.Wait()
}
