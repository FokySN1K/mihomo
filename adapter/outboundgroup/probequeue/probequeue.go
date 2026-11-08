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
package probequeue

import (
	"context"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// DefaultConcurrency is how many probes run at once unless the config says
// otherwise.
const DefaultConcurrency = 20

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
	probe Probe
}

type batch struct {
	q         *queue
	onResult  func(Result)
	probes    []Probe // probes that go through the ring
	priority  []Probe // probes that go through the priority lane
	next      int     // index of the next probe the ring hands out
	remaining int     // probes that have not reported yet, inline ones included
	done      chan struct{}
}

type queue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	limit    int
	running  int
	ring     []*batch
	cursor   int
	priority []probeItem
}

// defaultQueue is the process-wide queue. Groups share it: a per-group queue
// would sum back to the burst this package exists to prevent.
var defaultQueue = newQueue(DefaultConcurrency)

// Submit hands the probes of one check to the shared queue. onResult runs on
// the goroutine that finished the probe, once per probe, before the check is
// reported done.
func Submit(source string, probes []Probe, onResult func(Result)) *Check {
	return defaultQueue.submit(source, probes, onResult)
}

func (q *queue) submit(source string, probes []Probe, onResult func(Result)) *Check {
	b := &batch{q: q, onResult: onResult, done: make(chan struct{})}
	b.remaining = len(probes)
	if len(probes) == 0 {
		close(b.done)
		return &Check{done: b.done}
	}

	inline := make([]Probe, 0, len(probes))
	for _, probe := range probes {
		switch {
		case runsOwnCheck(probe.Proxy):
			// A group probes its own members through this queue, so running it
			// on a slot would hold that slot while it waits for the probes it
			// submitted itself. It runs on the caller's goroutine instead.
			inline = append(inline, probe)
		case probe.Priority:
			b.priority = append(b.priority, probe)
		default:
			b.probes = append(b.probes, probe)
		}
	}

	log.Debugln("Probe queue: %s queued %d probes", source, len(probes))

	q.mu.Lock()
	for _, probe := range b.priority {
		q.priority = append(q.priority, probeItem{batch: b, probe: probe})
	}
	if len(b.probes) > 0 {
		q.ring = append(q.ring, b)
	}
	q.cond.Broadcast()
	q.mu.Unlock()

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
	q := &queue{limit: limit}
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
	return len(q.priority) > 0 || len(q.ring) > 0
}

// popLocked takes the next probe: the priority lane first, then the ring of
// active checks one probe per check in turn, so every group starts getting
// answers in the first wave instead of after the check ahead of it. A check
// leaves the ring once its last probe is handed out, not when it finishes.
func (q *queue) popLocked() probeItem {
	if len(q.priority) > 0 {
		item := q.priority[0]
		q.priority = q.priority[1:]
		return item
	}

	if q.cursor >= len(q.ring) {
		q.cursor = 0
	}
	b := q.ring[q.cursor]
	item := probeItem{batch: b, probe: b.probes[b.next]}
	b.next++
	if b.next == len(b.probes) {
		q.ring = append(q.ring[:q.cursor], q.ring[q.cursor+1:]...)
		if q.cursor >= len(q.ring) {
			q.cursor = 0
		}
	} else {
		q.cursor++
	}
	return item
}

func (q *queue) runProbe(item probeItem) {
	delay, err := measure(item.probe)

	// The slot is freed before the result is delivered: the callback may take
	// a group lock, and the next probe must not wait for it.
	q.mu.Lock()
	q.running--
	q.cond.Signal()
	q.mu.Unlock()

	finish(item.batch, Result{Name: item.probe.Proxy.Name(), Delay: delay, Err: err})
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

// finish delivers one result and closes the check when it was the last one.
// Delivery comes first, so a caller woken by Wait already sees every result.
func finish(b *batch, result Result) {
	if b.onResult != nil {
		b.onResult(result)
	}

	b.q.mu.Lock()
	b.remaining--
	last := b.remaining == 0
	if last {
		close(b.done)
	}
	b.q.mu.Unlock()
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
