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
package probequeue

import (
	"context"
	"reflect"
	"sync"
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
	// lanePriority is a single probe a caller is waiting on: the UI test
	// button, a recovery probe on the dial path.
	lanePriority
)

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
// lane exists to prevent. An urgent probe follows the same rule against the
// background rings: joining a sweep that has not started would make a group
// with nothing to route by wait for the whole sweep.
func (e *probeEntry) joinable(probe Probe, lane lane) bool {
	switch {
	case probe.Priority:
		return e.running || e.lane == lanePriority
	case lane == laneUrgent:
		return e.running || e.lane >= laneUrgent
	default:
		return true
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
	inflight   map[probeKey]*probeEntry
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
	return len(q.priority) > 0 || len(q.urgent) > 0 || len(q.foreground) > 0 || len(q.ring) > 0
}

// popLocked takes the next probe: the priority lane first, then the rings of
// urgent, foreground and background checks, one probe per check in turn inside
// each ring, so every group starts getting answers in the first wave instead of
// after the check ahead of it. A check leaves its ring once its last probe is
// handed out, not when it finishes.
func (q *queue) popLocked() probeItem {
	if len(q.priority) > 0 {
		item := q.priority[0]
		q.priority = q.priority[1:]
		item.entry.running = true
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
		if b.alive == 1 && b.lane == laneUrgent {
			// The group has a member now: the rest of its check is ordinary
			// work and must stop delaying the groups that are still looking.
			b.q.demoteLocked(b)
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

// selfTester is implemented by every group adapter: a group answers a probe of
// its own by probing its members.
type selfTester interface {
	URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error)
}

func runsOwnCheck(proxy C.Proxy) bool {
	_, ok := proxy.Adapter().(selfTester)
	return ok
}
