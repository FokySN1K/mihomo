package dns

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/metacubex/mihomo/log"

	D "github.com/miekg/dns"
)

// upstreamHealth counts consecutive failures of every upstream address. Once
// the count reaches the limit the address is taken out of rotation for the
// cooldown, so a silent or broken nameserver stops costing a full timeout on
// each query while the rest of its group keeps serving.
type upstreamHealth struct {
	maxFails int
	cooldown time.Duration

	mu    sync.Mutex
	state map[string]*upstreamState
}

type upstreamState struct {
	fails    int
	downTill time.Time
}

func newUpstreamHealth(maxFails int, cooldown time.Duration) *upstreamHealth {
	if maxFails <= 0 {
		return nil
	}
	return &upstreamHealth{
		maxFails: maxFails,
		cooldown: cooldown,
		state:    make(map[string]*upstreamState),
	}
}

// pick returns the clients that may be queried right now. An empty result means
// every upstream in the group is cooling down; the caller decides whether to
// fail open or hand the query to the fallback.
func (h *upstreamHealth) pick(clients []dnsClient) []dnsClient {
	if h == nil {
		return clients
	}
	alive := make([]dnsClient, 0, len(clients))
	for _, c := range clients {
		if h.available(c.Address()) {
			alive = append(alive, c)
		}
	}
	return alive
}

// available reports whether addr may be queried. An expired cooldown gives the
// upstream a clean slate, so it is retried with a fresh failure count.
func (h *upstreamHealth) available(addr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	state, ok := h.state[addr]
	if !ok || state.downTill.IsZero() {
		return true
	}
	if time.Now().Before(state.downTill) {
		return false
	}
	state.downTill = time.Time{}
	state.fails = 0
	return true
}

// report records the outcome of one exchange. A canceled exchange says nothing
// about the upstream: another member of the batch won the race and canceled
// this one, so it is not counted as a failure.
func (h *upstreamHealth) report(addr string, msg *D.Msg, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	failed := err != nil || (msg != nil && (msg.Rcode == D.RcodeServerFailure || msg.Rcode == D.RcodeRefused))
	h.mu.Lock()
	defer h.mu.Unlock()
	state, ok := h.state[addr]
	if !ok {
		state = &upstreamState{}
		h.state[addr] = state
	}
	if !failed {
		state.fails = 0
		state.downTill = time.Time{}
		return
	}
	state.fails++
	if state.fails >= h.maxFails && state.downTill.IsZero() {
		state.downTill = time.Now().Add(h.cooldown)
		log.Warnln("[DNS] %s failed %d times in a row, skipping it for %s", addr, state.fails, h.cooldown)
	}
}

func (h *upstreamHealth) reset() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state = make(map[string]*upstreamState)
}

// trackedClient feeds the outcome of every exchange into the shared health map.
type trackedClient struct {
	dnsClient
	health *upstreamHealth
}

func (c *trackedClient) ExchangeContext(ctx context.Context, m *D.Msg) (*D.Msg, error) {
	msg, err := c.dnsClient.ExchangeContext(ctx, m)
	c.health.report(c.dnsClient.Address(), msg, err)
	return msg, err
}

// trackClients wraps each upstream once so a single health map sees every
// exchange. RCode clients are virtual answers and are left alone, otherwise
// their intentional Rcode would be counted as a failure.
func trackClients(clients []dnsClient, health *upstreamHealth) []dnsClient {
	if health == nil {
		return clients
	}
	tracked := make([]dnsClient, len(clients))
	for i, c := range clients {
		if _, isRCode := c.(rcodeClient); isRCode {
			tracked[i] = c
			continue
		}
		tracked[i] = &trackedClient{dnsClient: c, health: health}
	}
	return tracked
}

// activeMain returns the main upstreams worth querying. When every one of them
// is cooling down the whole list comes back, so a stale mark cannot leave the
// resolver without a main server at all.
func (r *Resolver) activeMain() []dnsClient {
	if alive := r.health.pick(r.main); len(alive) != 0 {
		return alive
	}
	return r.main
}

// fallbackClients mirrors activeMain for the fallback group.
func (r *Resolver) fallbackClients() []dnsClient {
	if alive := r.health.pick(r.fallback); len(alive) != 0 {
		return alive
	}
	return r.fallback
}

// dirtyAnswer reports whether a main answer must give way to the fallback: an
// answer without addresses may be censored, and a matching fallback filter is
// the explicit sign of it.
func (r *Resolver) dirtyAnswer(msg *D.Msg) bool {
	if msg == nil {
		return true
	}
	ips := msgToIP(msg)
	if len(ips) == 0 {
		return true
	}
	for _, ip := range ips {
		if r.shouldIPFallback(ip) {
			return true
		}
	}
	return false
}

// detachedDeadline keeps the deadline of ctx but drops its cancellation, so the
// main batch can finish and report its real outcome after the hedge has already
// answered the caller.
func detachedDeadline(ctx context.Context) context.Context {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithoutCancel(ctx)
	}
	detached, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	context.AfterFunc(detached, cancel)
	return detached
}

// hedgedExchange races the fallback against a main group that has not answered
// within the hedge delay: whichever answers first wins, so a silent main costs
// one delay plus the fallback round trip instead of the full timeout. A dirty
// main answer still yields to the fallback, and a failed fallback leaves main
// the rest of the deadline.
func (r *Resolver) hedgedExchange(ctx context.Context, m *D.Msg, mainCh <-chan *result) (*D.Msg, error) {
	var fallbackCh <-chan *result
	var mainErr, fallbackErr error

	startFallback := func() {
		fallbackCh = r.asyncExchange(ctx, r.fallbackClients(), m)
	}

	hedge := time.NewTimer(r.hedgeDelay)
	defer hedge.Stop()

	select {
	case res := <-mainCh:
		mainCh = nil
		if res.Error != nil {
			mainErr = res.Error
		} else if !r.dirtyAnswer(res.Msg) {
			return res.Msg, nil
		}
		startFallback()
	case <-hedge.C:
		startFallback()
	}

	for mainCh != nil || fallbackCh != nil {
		select {
		case res := <-mainCh:
			mainCh = nil
			if res.Error != nil {
				mainErr = res.Error
				continue
			}
			if fallbackCh == nil || !r.dirtyAnswer(res.Msg) {
				return res.Msg, nil
			}
			// a censored main answer is worth nothing while the fallback is
			// still on its way; keep waiting for it
		case res := <-fallbackCh:
			fallbackCh = nil
			if res.Error != nil {
				fallbackErr = res.Error
				continue
			}
			return res.Msg, nil
		case <-ctx.Done():
			if fallbackErr != nil {
				return nil, fallbackErr
			}
			if mainErr != nil {
				return nil, mainErr
			}
			return nil, ctx.Err()
		}
	}

	if fallbackErr != nil {
		return nil, fallbackErr
	}
	if mainErr != nil {
		return nil, mainErr
	}
	return nil, errors.New("all DNS requests failed")
}
