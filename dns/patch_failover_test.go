package dns

import (
	"context"
	"errors"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	D "github.com/miekg/dns"
)

func queryFor(host string) *D.Msg {
	query := new(D.Msg)
	query.SetQuestion(D.Fqdn(host), D.TypeA)
	return query
}

// blackholeClient accepts a query and never answers, like an upstream behind a
// whitelist that drops DNS traffic.
type blackholeClient struct {
	address string
}

func (c *blackholeClient) ExchangeContext(ctx context.Context, _ *D.Msg) (*D.Msg, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *blackholeClient) Address() string { return c.address }

func (c *blackholeClient) ResetConnection() {}

type countingFailingClient struct {
	address string
	calls   atomic.Int32
}

func (c *countingFailingClient) ExchangeContext(context.Context, *D.Msg) (*D.Msg, error) {
	c.calls.Add(1)
	return nil, errors.New("upstream unreachable")
}

func (c *countingFailingClient) Address() string { return c.address }

func (c *countingFailingClient) ResetConnection() {}

func TestFallbackHedgeAnswersWhileMainIsSilent(t *testing.T) {
	r := &Resolver{
		main:       []dnsClient{&blackholeClient{address: "udp://192.0.2.1:53"}},
		fallback:   []dnsClient{&answeringClient{address: "udp://192.0.2.2:53", ip: "198.51.100.9"}},
		hedgeDelay: 100 * time.Millisecond,
		cache:      Config{}.newCache(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	msg, err := r.ipExchange(ctx, queryFor("hedge.example"))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if elapsed > time.Second {
		t.Fatalf("the hedge answered in %s, want close to the 100ms delay", elapsed)
	}
	if ips := msgToIP(msg); len(ips) != 1 || ips[0].String() != "198.51.100.9" {
		t.Fatalf("answer = %v, want the fallback address", ips)
	}
}

func TestWithoutHedgeTheCallerWaitsForMain(t *testing.T) {
	r := &Resolver{
		main:     []dnsClient{&blackholeClient{address: "udp://192.0.2.1:53"}},
		fallback: []dnsClient{&answeringClient{address: "udp://192.0.2.2:53", ip: "198.51.100.9"}},
		cache:    Config{}.newCache(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _ = r.ipExchange(ctx, queryFor("no-hedge.example"))
	if elapsed := time.Since(start); elapsed < 350*time.Millisecond {
		t.Fatalf("answered in %s, want to wait for the main group deadline", elapsed)
	}
}

func TestDeadNameserverIsSkippedAfterMaxFails(t *testing.T) {
	health := newUpstreamHealth(2, time.Minute)
	dead := &countingFailingClient{address: "udp://192.0.2.1:53"}
	r := &Resolver{
		main:       trackClients([]dnsClient{dead}, health),
		fallback:   []dnsClient{&answeringClient{address: "udp://192.0.2.2:53", ip: "198.51.100.9"}},
		hedgeDelay: 50 * time.Millisecond,
		health:     health,
		cache:      Config{}.newCache(),
	}

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := r.ipExchange(ctx, queryFor("dead.example"))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if ips := msgToIP(msg); len(ips) != 1 || ips[0].String() != "198.51.100.9" {
			t.Fatalf("query %d answer = %v, want the fallback address", i, ips)
		}
	}

	if calls := dead.calls.Load(); calls != 2 {
		t.Fatalf("the dead upstream was queried %d times, want 2: a cooled-down server must be skipped", calls)
	}
}

type loopbackMatcher struct{}

func (loopbackMatcher) MatchIp(ip netip.Addr) bool { return ip.IsLoopback() }

func TestCensoredMainAnswerGivesWayToFallback(t *testing.T) {
	r := &Resolver{
		main:              []dnsClient{&answeringClient{address: "udp://192.0.2.1:53", ip: "127.0.0.1"}},
		fallback:          []dnsClient{&answeringClient{address: "udp://192.0.2.2:53", ip: "198.51.100.9"}},
		fallbackIPFilters: []C.IpMatcher{loopbackMatcher{}},
		hedgeDelay:        100 * time.Millisecond,
		cache:             Config{}.newCache(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg, err := r.ipExchange(ctx, queryFor("censored.example"))
	if err != nil {
		t.Fatal(err)
	}
	if ips := msgToIP(msg); len(ips) != 1 || ips[0].String() != "198.51.100.9" {
		t.Fatalf("answer = %v, want the fallback address", ips)
	}
}

func TestCanceledLoserIsNotCountedAsFailure(t *testing.T) {
	health := newUpstreamHealth(1, time.Minute)
	fast := &answeringClient{address: "udp://192.0.2.1:53", ip: "198.51.100.7"}
	slow := &blackholeClient{address: "udp://192.0.2.9:53"}
	clients := trackClients([]dnsClient{slow, fast}, health)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if msg, _, err := batchExchange(ctx, clients, queryFor("race.example")); err != nil || msg == nil {
		t.Fatalf("batchExchange = %v, %v", msg, err)
	}

	if !health.available(slow.address) {
		t.Fatal("a canceled loser was counted as a failure")
	}
}

func TestHedgedSilentMainIsStillCounted(t *testing.T) {
	health := newUpstreamHealth(1, time.Minute)
	main := trackClients([]dnsClient{&blackholeClient{address: "udp://192.0.2.1:53"}}, health)
	r := &Resolver{
		main:       main,
		fallback:   []dnsClient{&answeringClient{address: "udp://192.0.2.2:53", ip: "198.51.100.9"}},
		hedgeDelay: 50 * time.Millisecond,
		health:     health,
		cache:      Config{}.newCache(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := r.ipExchange(ctx, queryFor("learn.example")); err != nil {
		t.Fatal(err)
	}

	// The hedge answered at once, but main keeps running on its own deadline;
	// the early return must not hide that it never answered.
	deadline := time.Now().Add(2 * time.Second)
	for health.available("udp://192.0.2.1:53") {
		if time.Now().After(deadline) {
			t.Fatal("a silent main was not counted after the hedge answered")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCooldownExpiryGivesUpstreamAFreshChance(t *testing.T) {
	const addr = "udp://192.0.2.1:53"
	health := newUpstreamHealth(1, 50*time.Millisecond)
	health.report(addr, nil, errors.New("upstream unreachable"))

	if health.available(addr) {
		t.Fatal("an upstream that reached max fails is still queried")
	}
	time.Sleep(60 * time.Millisecond)
	if !health.available(addr) {
		t.Fatal("an expired cooldown did not give the upstream a fresh chance")
	}
}
