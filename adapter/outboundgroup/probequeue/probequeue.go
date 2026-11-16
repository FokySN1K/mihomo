// Package probequeue runs the health probes of every group through one shared
// queue: a fixed number of probes at a time, round-robin between the checks
// that submitted them, and a priority lane for the probes a dial is waiting on.
//
// It exists because per-group limits add up. Five groups probing twenty members
// each are a hundred concurrent handshakes, and a burst-intolerant path (a
// mobile carrier, a relay in front of the nodes) answers that with timeouts for
// everyone. A group still decides what to probe and how long a member may take;
// the queue only decides when the probe runs, so the load on the network no
// longer depends on how many groups a profile happens to define.
//
// Two checks that ask for the same proxy, URL, accepted statuses and budget ask
// for the same answer: the second one waits on the probe already in flight
// instead of loading the path a second time.
//
// A scheduled cold check of a url-test-pool can also join the ordered chain:
// the profile numbers its groups with `ordered`, the queue serves one wave of
// numbers at a time, and a dial that arrives for a group promotes its check to
// the head instead of waiting behind the waves ahead of it.
package probequeue

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// DefaultConcurrency is how many probes run at once unless the config says
// otherwise.
const DefaultConcurrency = 40

// defaultTestTimeout is the budget of a single test whose caller did not set a
// deadline on its context.
const defaultTestTimeout = 5 * time.Second

// maxConcurrency keeps a mistyped profile from turning the shared queue back
// into the burst it replaced.
const maxConcurrency = 64

// Probe is one measurement a group wants: a single proxy against one URL. The
// timeout is the probe's only budget; a batch has none.
type Probe struct {
	Proxy    C.Proxy
	URL      string
	Expected utils.IntRanges[uint16]
	Timeout  time.Duration
	// Priority probes overtake the round-robin: a dial is waiting on them, so
	// they must not queue behind a whole sweep of scheduled checks.
	Priority bool
}

// Result is one finished probe. Err is a verdict about the proxy, not about the
// caller: a probe that ran out of its own timeout is a dead node, the same as
// one whose handshake was refused.
type Result struct {
	Name  string
	Delay uint16
	Err   error
}

// Check is a batch of probes handed to the queue. Wait bounds only the answer
// the caller gets: probes keep running to their own timeout after the caller is
// gone, because their results still feed the group they belong to.
type Check struct {
	done chan struct{}
}

// Wait blocks until every probe of the check has finished and reports whether
// that happened within the caller's budget.
func (c *Check) Wait(ctx context.Context) bool {
	select {
	case <-c.done:
		return true
	default:
	}

	select {
	case <-c.done:
		return true
	case <-ctx.Done():
		return false
	}
}

type probeItem struct {
	batch *batch
	entry *probeEntry
}

// lane orders the work of the queue. A probe a dial is waiting on outranks the
// probes of a group that has nothing to route by, those outrank the checks of a
// group that feeds routing decisions, and the background sweeps come last.
// A higher value is served sooner.
type lane int

const (
	// laneRing is the background sweep: a lazy group that woke up for traffic,
	// or a provider tick.
	laneRing lane = iota
	// laneForeground is a check whose answers the next connections route by.
	laneForeground
	// laneUrgent is a check of a group that has nothing to route by at all: a
	// dial waits for its first result, so it must not queue behind the sweeps.
	laneUrgent
	// laneChain is a scheduled cold check that takes part in the ordered
	// chain: only one wave of the chain is served at a time, so the profile
	// decides which group finds its member first. A dial that arrives for the
	// group promotes its check to the head of the chain.
	laneChain
	// lanePriority is a single probe a caller is waiting on: the UI test
	// button, a recovery probe on the dial path.
	lanePriority
)

// chainPlace is where a group stands in the ordered chain: its explicit
// `ordered` number, or its index in `proxy-groups` when it set none. Groups
// with equal numbers share a wave; the groups without a number follow all the
// explicit ones in declaration order and each stands alone, so the chain
// never interleaves them with the numbers.
type chainPlace struct {
	explicit bool
	value    int
	declared int
}

// sameWave reports whether two places are served together: equal explicit
// numbers are one wave, and a tail place is a wave of its own.
func (p chainPlace) sameWave(other chainPlace) bool {
	if p.explicit != other.explicit {
		return false
	}
	if p.explicit {
		return p.value == other.value
	}
	return p.declared == other.declared
}

// before orders the places of the chain: explicit numbers ascending, then the
// groups that set none in declaration order.
func (p chainPlace) before(other chainPlace) bool {
	if p.explicit != other.explicit {
		return p.explicit
	}
	if p.explicit && p.value != other.value {
		return p.value < other.value
	}
	return p.declared < other.declared
}

// maxDeclaredChain bounds the implicit places: a declaration index never comes
// close to it, so an unregistered source always sorts behind a profile group.
const maxDeclaredChain = 1 << 30

// The chain registry is rebuilt by the config parser on every profile load.
// The active order keeps serving the running profile until the new profile
// parsed, so a config that fails to parse cannot leave the queue without one.
var (
	chainMu      sync.RWMutex
	chainPlaces  map[string]chainPlace
	chainPending map[string]chainPlace
	chainUnknown atomic.Int64
)

// ProbeOrder is how the queue runs the scheduled cold checks of the groups
// that take part in the ordered chain.
type ProbeOrder int32

const (
	// OrderSequential runs one wave of the chain at a time: the explicit
	// numbers ascending, then the groups that set none in declaration order.
	OrderSequential ProbeOrder = iota
	// OrderParallel keeps the pre-chain behavior: every cold check shares the
	// urgent lane in round-robin.
	OrderParallel
)

func (o ProbeOrder) String() string {
	if o == OrderParallel {
		return "parallel"
	}
	return "sequential"
}

// ParseProbeOrder reads the `probe-order` option.
func ParseProbeOrder(value string) (ProbeOrder, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "sequential":
		return OrderSequential, nil
	case "parallel":
		return OrderParallel, nil
	}
	return OrderSequential, fmt.Errorf("unsupported probe-order %q, want sequential or parallel", value)
}

var probeOrder atomic.Int32

// SetProbeOrder applies the `probe-order` option. Switching to parallel drains
// the chain into the urgent lane: no check may be left waiting in a lane the
// queue no longer serves.
func SetProbeOrder(order ProbeOrder) {
	if order != OrderParallel {
		order = OrderSequential
	}
	if ProbeOrder(probeOrder.Swap(int32(order))) == order {
		return
	}
	if order == OrderParallel {
		defaultQueue.drainChain()
	}
	log.Debugln("Probe queue: probe order %s", order)
}

// GetProbeOrder reports the order the queue is running with.
func GetProbeOrder() ProbeOrder {
	return ProbeOrder(probeOrder.Load())
}

// BeginChain opens a new order registry for the profile that is being parsed.
// The active order keeps serving the running profile until CommitChain swaps
// the new one in.
func BeginChain() {
	chainMu.Lock()
	chainPending = make(map[string]chainPlace)
	chainMu.Unlock()
}

// RegisterChain places a url-test-pool group in the chain. ordered is the
// group's `ordered` option (nil when it set none), declared is the index of
// the group in `proxy-groups`.
func RegisterChain(name string, ordered *int, declared int) {
	place := chainPlace{declared: declared}
	if ordered != nil {
		place.explicit = true
		place.value = *ordered
	}
	chainMu.Lock()
	if chainPending == nil {
		chainPending = make(map[string]chainPlace)
	}
	chainPending[name] = place
	chainMu.Unlock()
}

// CommitChain makes the registry built since BeginChain the active one.
func CommitChain() {
	chainMu.Lock()
	if chainPending != nil {
		chainPlaces = chainPending
		chainPending = nil
	}
	chainMu.Unlock()
}

// ResetChain forgets every registered group.
func ResetChain() {
	chainMu.Lock()
	chainPlaces = nil
	chainPending = nil
	chainMu.Unlock()
}

// chainPlaceOf looks a group up. A source the profile did not register (a
// group a host built itself, a test) takes its own place at the tail.
func chainPlaceOf(source string) chainPlace {
	chainMu.RLock()
	place, ok := chainPlaces[source]
	chainMu.RUnlock()
	if ok {
		return place
	}
	return chainPlace{declared: maxDeclaredChain + int(chainUnknown.Add(1))}
}

// probeKey identifies one measurement. The same proxy, URL, accepted statuses
// and budget is the same question, whatever check asks it.
type probeKey struct {
	proxy    C.Proxy
	url      string
	expected string
	timeout  time.Duration
}

// probeEntry is one measurement the queue believes is worth running. Checks
// that submit the same probe while this one is in flight wait on the entry
// instead of starting a second measurement.
type probeEntry struct {
	key     probeKey
	probe   Probe
	waiters []*batch
	running bool
	// lane is the lane the entry waits in: joining it is as good as running it
	// only when its lane is served no later than the caller's.
	lane lane
}

// joinable reports whether a check that submitted the same probe can wait on
// this entry. A priority probe only joins a measurement that already runs or
// one that waits in the priority lane itself: joining an entry of the ring
// would make the dial wait behind the ring, which is exactly what the priority
// lane exists to prevent. An urgent or chained probe follows the same rule
// against the lanes the queue serves later: joining a sweep that has not
// started would make a group with nothing to route by wait for the whole
// sweep, and joining an urgent entry would make an ordered wave wait behind
// work that comes after it.
func (e *probeEntry) joinable(probe Probe, lane lane) bool {
	switch {
	case probe.Priority:
		return e.running || e.lane == lanePriority
	case lane >= laneUrgent:
		return e.running || e.lane.servedNoLaterThan(lane)
	default:
		return true
	}
}

// servedNoLaterThan reports whether the queue serves this lane no later than
// the other one. The serving order is priority, chain, urgent, foreground and
// the background ring.
func (l lane) servedNoLaterThan(other lane) bool {
	return laneServingRank(l) >= laneServingRank(other)
}

func laneServingRank(l lane) int {
	switch l {
	case lanePriority:
		return 4
	case laneChain:
		return 3
	case laneUrgent:
		return 2
	case laneForeground:
		return 1
	default:
		return 0
	}
}

type batch struct {
	q         *queue
	source    string
	onResult  func(Result)
	probes    []*probeEntry // probes that go through a ring
	priority  []*probeEntry // probes that go through the priority lane
	next      int           // index of the next probe the ring hands out
	remaining int           // probes that have not reported yet, inline ones included
	alive     int
	failed    int
	started   time.Time
	done      chan struct{}
	// lane is the lane the batch waits in, rest is the lane the remaining
	// probes drop to once the batch measured a member: an urgent check stops
	// delaying the groups that are still looking for one.
	lane lane
	rest lane
	// place is where the group stands in the ordered chain, and promoted
	// marks a check a dial has arrived for: it goes ahead of the waves that
	// were ahead of it.
	place    chainPlace
	promoted bool
}

type queue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	limit   int
	running int
	// urgent holds the checks of groups that have nothing to route by; a dial
	// waits for their first answer, so they are served before both rings.
	urgent       []*batch
	urgentCursor int
	// foreground holds the checks of groups that do not wait for traffic
	// (lazy: false): their answers are what the next connections route by, so
	// they are served before the background sweeps.
	foreground []*batch
	fgCursor   int
	ring       []*batch
	cursor     int
	priority   []probeItem
	// chain holds the scheduled cold checks of the ordered groups. One wave of
	// equal places is served at a time: a check stays here until its last probe
	// is handed out or its wave found a member.
	chain []*batch
	// promoted remembers the groups a dial has arrived for before their check
	// reached the queue: the batch picks the request up when it is submitted.
	promoted map[string]struct{}
	inflight map[probeKey]*probeEntry
}

// defaultQueue is the process-wide queue. Groups share it: a per-group queue
// would sum back to the burst this package exists to prevent.
var defaultQueue = newQueue(DefaultConcurrency)

// Submit hands the probes of one check to the shared queue. onResult runs on
// the goroutine that finished the probe, once per probe, before the check is
// reported done.
func Submit(source string, probes []Probe, onResult func(Result)) *Check {
	return defaultQueue.submit(source, false, probes, onResult)
}

// SubmitForeground hands the probes of one check to the queue ahead of the
// background sweeps. A group that checks without `lazy` feeds the connections
// the profile routes by itself, so its answers are needed before the checks of
// groups that wait for traffic; the two lanes still keep their own round-robin.
func SubmitForeground(source string, probes []Probe, onResult func(Result)) *Check {
	return defaultQueue.submit(source, true, probes, onResult)
}

// SubmitUrgent hands the probes of a group that has nothing to route by to the
// urgent lane: a dial waits for its first result, so it goes ahead of both
// rings. Once a probe reports a member, the remaining ones drop to the
// foreground lane (or the background ring when `foreground` is false), so the
// check stops delaying the groups that are still looking for a member.
func SubmitUrgent(source string, foreground bool, probes []Probe, onResult func(Result)) *Check {
	rest := laneRing
	if foreground {
		rest = laneForeground
	}
	return defaultQueue.submitInLane(source, laneUrgent, rest, probes, onResult)
}

// SubmitChain hands a scheduled cold check of a url-test-pool to the ordered
// chain: the profile numbers its groups with `ordered`, one wave of the chain
// is served at a time, so the groups it named first find their members first.
// With `probe-order: parallel` the check goes to the urgent lane as before and
// the chain plays no part.
func SubmitChain(source string, foreground bool, probes []Probe, onResult func(Result)) *Check {
	if GetProbeOrder() == OrderParallel {
		return SubmitUrgent(source, foreground, probes, onResult)
	}
	return defaultQueue.submitChain(source, foreground, probes, onResult)
}

func (q *queue) submitChain(source string, foreground bool, probes []Probe, onResult func(Result)) *Check {
	rest := laneRing
	if foreground {
		rest = laneForeground
	}
	return q.submitInLane(source, laneChain, rest, probes, onResult)
}

// Promote moves the scheduled check of a group to the head of the ordered
// chain: a dial has arrived for the group, so its answers are needed now and
// not after the waves ahead of it. A check that already runs or does not take
// part in the chain is left alone.
func Promote(source string) {
	defaultQueue.promote(source)
}

// TestProxy measures one proxy for a caller that is waiting on the answer: the
// UI test button, a recovery probe on the dial path. The test is one probe of a
// one-probe check in the priority lane, so it never adds a fan-out of its own
// and never waits behind a whole sweep. The caller's context bounds only how
// long the caller waits; the probe keeps its own budget.
func TestProxy(ctx context.Context, proxy C.Proxy, url string, expected utils.IntRanges[uint16]) (uint16, error) {
	timeout := defaultTestTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 {
			timeout = remaining
		}
	}
	return TestProxyWithBudget(ctx, proxy, url, expected, timeout)
}

// TestProxyWithBudget is TestProxy with the probe budget spelled out. A caller
// that knows the budget a group's own check uses keeps the probe key equal, so
// the queue shares the measurement instead of running a second one beside the
// sweep.
func TestProxyWithBudget(ctx context.Context, proxy C.Proxy, url string, expected utils.IntRanges[uint16], timeout time.Duration) (uint16, error) {
	if timeout <= 0 {
		timeout = defaultTestTimeout
	}

	var delay uint16
	var err error
	check := Submit(proxy.Name(), []Probe{{
		Proxy:    proxy,
		URL:      url,
		Expected: expected,
		Timeout:  timeout,
		Priority: true,
	}}, func(result Result) {
		delay, err = result.Delay, result.Err
	})
	if !check.Wait(ctx) {
		return 0, ctx.Err()
	}
	return delay, err
}

func (q *queue) submit(source string, foreground bool, probes []Probe, onResult func(Result)) *Check {
	lane := laneRing
	if foreground {
		lane = laneForeground
	}
	return q.submitInLane(source, lane, lane, probes, onResult)
}

func (q *queue) submitInLane(source string, lane, rest lane, probes []Probe, onResult func(Result)) *Check {
	b := &batch{q: q, source: source, onResult: onResult, started: time.Now(), done: make(chan struct{}), lane: lane, rest: rest}
	if lane == laneChain {
		b.place = chainPlaceOf(source)
	}
	b.remaining = len(probes)
	if len(probes) == 0 {
		close(b.done)
		return &Check{done: b.done}
	}

	inline := make([]Probe, 0, len(probes))
	shared := 0

	// The queue map is taken before the probes are placed, so a probe that the
	// queue already runs is joined rather than measured twice.
	q.mu.Lock()
	for _, probe := range probes {
		if runsOwnCheck(probe.Proxy) {
			// A group probes its own members through this queue, so running it
			// on a slot would hold that slot while it waits for the probes it
			// submitted itself. It runs on the caller's goroutine instead.
			inline = append(inline, probe)
			continue
		}

		probeLane := lane
		if probe.Priority {
			probeLane = lanePriority
		}

		key, keyed := probeKeyOf(probe)
		if keyed {
			if entry, ok := q.inflight[key]; ok && entry.joinable(probe, lane) {
				entry.waiters = append(entry.waiters, b)
				shared++
				continue
			}
		}

		entry := &probeEntry{key: key, probe: probe, waiters: []*batch{b}, lane: probeLane}
		if probe.Priority {
			b.priority = append(b.priority, entry)
		} else {
			b.probes = append(b.probes, entry)
		}
		if keyed {
			// The map keeps the most urgent measurement of a key: a probe that
			// could not join the queued ring twin takes the key over, so the
			// next urgent ask joins the urgent one instead of the sweep.
			if current, ok := q.inflight[key]; !ok || probeLane > current.lane {
				q.inflight[key] = entry
			}
		}
	}

	for _, entry := range b.priority {
		q.priority = append(q.priority, probeItem{batch: b, entry: entry})
	}
	if len(b.probes) > 0 {
		switch lane {
		case laneUrgent:
			q.urgent = append(q.urgent, b)
		case laneChain:
			b.promoted = q.takePromotionLocked(b.source)
			q.insertChainLocked(b)
		case laneForeground:
			q.foreground = append(q.foreground, b)
		default:
			q.ring = append(q.ring, b)
		}
	}
	q.cond.Broadcast()
	q.mu.Unlock()

	log.Debugln("Probe queue: %s queued %d probes (%d already in flight)", source, len(probes), shared)

	for _, probe := range inline {
		runInline(b, probe)
	}

	return &Check{done: b.done}
}

// SetConcurrency applies the `probe-concurrency` option. Probes already
// running keep their slot; the new limit decides how many start next.
func SetConcurrency(n int) {
	if n <= 0 {
		n = DefaultConcurrency
	}
	if n > maxConcurrency {
		log.Warnln("Probe concurrency %d is above the limit, using %d", n, maxConcurrency)
		n = maxConcurrency
	}
	defaultQueue.setConcurrency(n)
}

func (q *queue) setConcurrency(n int) {
	q.mu.Lock()
	changed := q.limit != n
	q.limit = n
	q.cond.Broadcast()
	q.mu.Unlock()
	if changed {
		log.Debugln("Probe queue: concurrency %d", n)
	}
}

// GetConcurrency reports the limit the queue is running with.
func GetConcurrency() int {
	q := defaultQueue
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.limit
}

func newQueue(limit int) *queue {
	q := &queue{limit: limit, inflight: make(map[probeKey]*probeEntry)}
	q.cond = sync.NewCond(&q.mu)
	go q.dispatch()
	return q
}

// dispatch hands out probes as slots free up. The workers are plain goroutines
// rather than a fixed pool, so a limit that changes at runtime needs no restart.
func (q *queue) dispatch() {
	for {
		q.mu.Lock()
		for q.running >= q.limit || !q.hasWorkLocked() {
			q.cond.Wait()
		}
		item := q.popLocked()
		q.running++
		q.mu.Unlock()

		go q.runProbe(item)
	}
}

func (q *queue) hasWorkLocked() bool {
	return len(q.priority) > 0 || len(q.chain) > 0 || len(q.urgent) > 0 || len(q.foreground) > 0 || len(q.ring) > 0
}

// popLocked takes the next probe: the priority lane first, then the ordered
// chain, then the rings of urgent, foreground and background checks, one probe
// per check in turn inside each ring, so every group starts getting answers in
// the first wave instead of after the check ahead of it. A check leaves its
// ring once its last probe is handed out, not when it finishes.
func (q *queue) popLocked() probeItem {
	if len(q.priority) > 0 {
		item := q.priority[0]
		q.priority = q.priority[1:]
		item.entry.running = true
		return item
	}

	if item, ok := q.popChainLocked(); ok {
		return item
	}
	if item, ok := q.popRingLocked(&q.urgent, &q.urgentCursor); ok {
		return item
	}
	if item, ok := q.popRingLocked(&q.foreground, &q.fgCursor); ok {
		return item
	}
	item, _ := q.popRingLocked(&q.ring, &q.cursor)
	return item
}

// popRingLocked hands out one probe of the ring's current batch and reports
// whether there was any work at all.
func (q *queue) popRingLocked(ring *[]*batch, cursor *int) (probeItem, bool) {
	if len(*ring) == 0 {
		return probeItem{}, false
	}

	if *cursor >= len(*ring) {
		*cursor = 0
	}
	b := (*ring)[*cursor]
	entry := b.probes[b.next]
	entry.running = true
	item := probeItem{batch: b, entry: entry}
	b.next++
	if b.next == len(b.probes) {
		*ring = append((*ring)[:*cursor], (*ring)[*cursor+1:]...)
		if *cursor >= len(*ring) {
			*cursor = 0
		}
	} else {
		*cursor++
	}
	return item, true
}

func (q *queue) runProbe(item probeItem) {
	delay, err := measure(item.entry.probe)
	result := Result{Name: item.entry.probe.Proxy.Name(), Delay: delay, Err: err}

	// The slot is freed before the result is delivered: the callback may take
	// a group lock, and the next probe must not wait for it. Every check that
	// joined this measurement gets the result.
	q.mu.Lock()
	q.running--
	q.cond.Signal()
	waiters := item.entry.waiters
	if current, ok := q.inflight[item.entry.key]; ok && current == item.entry {
		delete(q.inflight, item.entry.key)
	}
	q.mu.Unlock()

	for _, b := range waiters {
		finish(b, result)
	}
}

// runInline measures a probe that cannot go through the ring. It runs on the
// caller's goroutine and does not count against the limit, so the nested probes
// it submits can always find a free slot.
func runInline(b *batch, probe Probe) {
	delay, err := measure(probe)
	finish(b, Result{Name: probe.Proxy.Name(), Delay: delay, Err: err})
}

func measure(probe Probe) (uint16, error) {
	// The probe's own timeout is the only limit it has: a caller that stops
	// waiting must not turn a slow member into a dead one.
	ctx, cancel := context.WithTimeout(context.Background(), probe.Timeout)
	defer cancel()
	return probe.Proxy.URLTest(ctx, probe.URL, probe.Expected)
}

// probeKeyOf reports the identity of a probe. A proxy that is not a comparable
// value cannot be told apart from another one, so its probes are never shared.
func probeKeyOf(probe Probe) (probeKey, bool) {
	if probe.Proxy == nil || !reflect.TypeOf(probe.Proxy).Comparable() {
		return probeKey{}, false
	}
	return probeKey{
		proxy:    probe.Proxy,
		url:      probe.URL,
		expected: probe.Expected.String(),
		timeout:  probe.Timeout,
	}, true
}

// finish delivers one result and closes the check when it was the last one.
// Delivery comes first, so a caller woken by Wait already sees every result.
func finish(b *batch, result Result) {
	if b.onResult != nil {
		b.onResult(result)
	}

	b.q.mu.Lock()
	if result.Err != nil {
		b.failed++
	} else {
		b.alive++
		if b.alive == 1 {
			switch b.lane {
			case laneUrgent:
				// The group has a member now: the rest of its check is
				// ordinary work and must stop delaying the groups that are
				// still looking.
				b.q.demoteLocked(b)
			case laneChain:
				// The wave found its member: the rest of its probes and the
				// checks behind it in the chain are ordinary work now.
				b.q.demoteChainWaveLocked(b)
			}
		}
	}
	b.remaining--
	last := b.remaining == 0
	if last {
		close(b.done)
	}
	b.q.mu.Unlock()

	if last {
		log.Debugln("Probe queue: %s done: %d alive, %d failed in %s", b.source, b.alive, b.failed, time.Since(b.started).Round(time.Millisecond))
	}
}

// demoteLocked moves the remaining probes of an urgent check to its home lane
// once the check measured a member. The batch may already have left the urgent
// ring (its last probe was handed out), in which case only the lane is stamped:
// the probes still running keep their slot.
func (q *queue) demoteLocked(b *batch) {
	for i, queued := range q.urgent {
		if queued != b {
			continue
		}
		q.urgent = append(q.urgent[:i], q.urgent[i+1:]...)
		if q.urgentCursor > i {
			q.urgentCursor--
		}
		break
	}
	if q.urgentCursor >= len(q.urgent) {
		q.urgentCursor = 0
	}

	q.requeueRestLocked(b)
}

// requeueRestLocked moves a check that leaves the urgent or chain lane to its
// rest lane. The probes that were not handed out yet drop along, so the check
// stops delaying the groups that are still looking; a check that already gave
// every probe away only has its lane stamped.
func (q *queue) requeueRestLocked(b *batch) {
	b.lane = b.rest
	if b.next >= len(b.probes) {
		// every probe was handed out already: nothing is left to requeue
		return
	}
	for _, entry := range b.probes[b.next:] {
		entry.lane = b.rest
	}
	if b.rest == laneForeground {
		q.foreground = append(q.foreground, b)
	} else {
		q.ring = append(q.ring, b)
	}
}

// insertChainLocked puts a scheduled check in the chain behind the places that
// come before it. Equal places keep the order they arrived in; the wave itself
// is served round-robin.
func (q *queue) insertChainLocked(b *batch) {
	at := len(q.chain)
	for i, queued := range q.chain {
		if b.place.before(queued.place) {
			at = i
			break
		}
	}
	q.chain = append(q.chain, nil)
	copy(q.chain[at+1:], q.chain[at:])
	q.chain[at] = b
}

// removeChainLocked takes a check out of the chain.
func (q *queue) removeChainLocked(b *batch) {
	for i, queued := range q.chain {
		if queued == b {
			q.chain = append(q.chain[:i], q.chain[i+1:]...)
			return
		}
	}
}

// chainHeadLocked reports the check the chain serves now: a promoted check
// first, then the earliest place. A promoted check goes ahead of the waves
// still ahead of it for the same reason a dial outranks the schedule.
func (q *queue) chainHeadLocked() *batch {
	var head *batch
	for _, b := range q.chain {
		if head == nil || chainBeats(b, head) {
			head = b
		}
	}
	return head
}

// chainBeats reports whether candidate is served before current. Two checks of
// one wave keep the order they have, so the lookup sees the first one and the
// served check rotates to the tail.
func chainBeats(candidate, current *batch) bool {
	if candidate.promoted != current.promoted {
		return candidate.promoted
	}
	if candidate.place.sameWave(current.place) {
		return false
	}
	return candidate.place.before(current.place)
}

// popChainLocked hands out one probe of the wave the chain serves now: one
// probe per check of the wave in turn, and a check leaves the chain once its
// last probe is handed out. The next wave is served only when this one is
// empty, so the groups the profile ordered first get their answers first.
func (q *queue) popChainLocked() (probeItem, bool) {
	b := q.chainHeadLocked()
	if b == nil {
		return probeItem{}, false
	}

	entry := b.probes[b.next]
	entry.running = true
	item := probeItem{batch: b, entry: entry}
	b.next++
	// A check whose last probe is handed out leaves the chain now; the one
	// that still has probes to hand out goes to the tail, so the other checks
	// of its wave are served in turn.
	q.removeChainLocked(b)
	if b.next < len(b.probes) {
		q.chain = append(q.chain, b)
	}
	return item, true
}

// demoteChainWaveLocked moves the rest of a wave to the rest lane once one of
// its checks measured a member: the wave has something to route by, and the
// waves behind it must not wait for its remaining probes.
func (q *queue) demoteChainWaveLocked(b *batch) {
	demoted := false
	for _, queued := range append([]*batch(nil), q.chain...) {
		if !queued.place.sameWave(b.place) {
			continue
		}
		q.removeChainLocked(queued)
		q.requeueRestLocked(queued)
		if queued == b {
			demoted = true
		}
	}
	if !demoted {
		// The check gave every probe away already; only its lane is stamped.
		q.requeueRestLocked(b)
	}
}

// drainChain moves every check still waiting in the chain to the urgent ring.
// It runs when the profile switches `probe-order` to parallel: the chain lane
// is no longer served, and no check may be left waiting in it.
func (q *queue) drainChain() {
	q.mu.Lock()
	if len(q.chain) > 0 {
		for _, b := range q.chain {
			b.lane = laneUrgent
			for _, entry := range b.probes[b.next:] {
				entry.lane = laneUrgent
			}
			q.urgent = append(q.urgent, b)
		}
		q.chain = nil
		q.cond.Broadcast()
	}
	q.mu.Unlock()
}

// promote moves the check of a group to the head of the chain: a dial has
// arrived for the group, so its answers are no longer a background question.
// A check that is not waiting in the chain is left alone: it already runs or
// already left its lane.
func (q *queue) promote(source string) {
	q.mu.Lock()
	found := false
	for _, b := range q.chain {
		if b.source == source {
			found = true
			b.promoted = true
		}
	}
	if !found {
		// The check has not reached the queue yet: the batch picks the request
		// up when it is submitted.
		if q.promoted == nil {
			q.promoted = make(map[string]struct{})
		}
		q.promoted[source] = struct{}{}
	}
	q.cond.Broadcast()
	q.mu.Unlock()
}

// takePromotionLocked reports whether a dial asked for this group before its
// check reached the queue, and forgets the request: one dial promotes one
// check.
func (q *queue) takePromotionLocked(source string) bool {
	if len(q.promoted) == 0 {
		return false
	}
	if _, ok := q.promoted[source]; !ok {
		return false
	}
	delete(q.promoted, source)
	return true
}

// selfTester is implemented by every group adapter: a group answers a probe of
// its own by probing its members.
type selfTester interface {
	URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error)
}

func runsOwnCheck(proxy C.Proxy) bool {
	_, ok := proxy.Adapter().(selfTester)
	return ok
}
