package probequeue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

// fakeProxy satisfies the parts of C.Proxy the queue touches. The embedded nil
// interface keeps the rest of the method set out of the test.
type fakeProxy struct {
	C.Proxy
	name    string
	adapter C.ProxyAdapter
	probe   func(ctx context.Context) (uint16, error)
}

func (f *fakeProxy) Name() string { return f.name }

func (f *fakeProxy) Adapter() C.ProxyAdapter { return f.adapter }

func (f *fakeProxy) URLTest(ctx context.Context, _ string, _ utils.IntRanges[uint16]) (uint16, error) {
	return f.probe(ctx)
}

// fakeGroupAdapter makes a proxy look like a group to the queue: it answers a
// probe of its own by probing its members, so it must not run on a slot.
type fakeGroupAdapter struct {
	C.ProxyAdapter
}

func (g *fakeGroupAdapter) URLTest(context.Context, string, utils.IntRanges[uint16]) (map[string]uint16, error) {
	return map[string]uint16{}, nil
}

func newTestProxy(name string, probe func(ctx context.Context) (uint16, error)) *fakeProxy {
	return &fakeProxy{name: name, probe: probe}
}

func okProbe(delay uint16) func(context.Context) (uint16, error) {
	return func(context.Context) (uint16, error) { return delay, nil }
}

// startRecorder records the order in which probes of a test start.
type startRecorder struct {
	mu    sync.Mutex
	order []string
}

func (r *startRecorder) add(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, name)
}

func (r *startRecorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

func (r *startRecorder) index(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, recorded := range r.order {
		if recorded == name {
			return i
		}
	}
	return -1
}

func requireEventually(t *testing.T, condition func() bool, message string) {
	t.Helper()
	require.Eventually(t, condition, 2*time.Second, time.Millisecond, message)
}

// The limit is the whole point: no more probes run at once than the config
// allows, and the queue still runs them in parallel.
func TestQueueLimitsInFlight(t *testing.T) {
	q := newQueue(3)

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var delivered atomic.Int32

	probes := make([]Probe, 0, 12)
	for i := 0; i < 12; i++ {
		probes = append(probes, Probe{
			Proxy: newTestProxy(fmt.Sprintf("p%02d", i), func(context.Context) (uint16, error) {
				running := inFlight.Add(1)
				for {
					seen := maxInFlight.Load()
					if running <= seen || maxInFlight.CompareAndSwap(seen, running) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				inFlight.Add(-1)
				return 10, nil
			}),
			Timeout: time.Second,
		})
	}

	check := q.submit("sweep", false, probes, func(Result) { delivered.Add(1) })
	require.True(t, check.Wait(context.Background()))
	require.Equal(t, int32(12), delivered.Load())
	require.LessOrEqual(t, maxInFlight.Load(), int32(3), "the queue must not exceed its limit")
	require.Greater(t, maxInFlight.Load(), int32(1), "the queue must still probe in parallel")
}

// The ring serves one probe of a check per turn, so a check that starts later
// still gets its answers in the first waves instead of after the whole sweep
// ahead of it.
func TestPopLockedRoundRobinBetweenChecks(t *testing.T) {
	q := &queue{limit: 1}

	newBatch := func(q *queue, prefix string, count int) *batch {
		probes := make([]*probeEntry, 0, count)
		for i := 0; i < count; i++ {
			probes = append(probes, &probeEntry{probe: Probe{Proxy: newTestProxy(fmt.Sprintf("%s%d", prefix, i), okProbe(1))}})
		}
		return &batch{q: q, probes: probes}
	}

	q.ring = []*batch{newBatch(q, "a", 3), newBatch(q, "b", 3)}

	var order []string
	for q.hasWorkLocked() {
		order = append(order, q.popLocked().entry.probe.Proxy.Name())
	}
	require.Equal(t, []string{"a0", "b0", "a1", "b1", "a2", "b2"}, order)
}

// The foreground ring serves the checks of the groups that do not wait for
// traffic before the background sweeps, and keeps its own round-robin.
func TestPopLockedForegroundBeforeBackground(t *testing.T) {
	q := &queue{limit: 1}

	newBatch := func(prefix string, count int) *batch {
		probes := make([]*probeEntry, 0, count)
		for i := 0; i < count; i++ {
			probes = append(probes, &probeEntry{probe: Probe{Proxy: newTestProxy(fmt.Sprintf("%s%d", prefix, i), okProbe(1))}})
		}
		return &batch{q: q, probes: probes}
	}

	q.foreground = []*batch{newBatch("fa", 2), newBatch("fb", 2)}
	q.ring = []*batch{newBatch("bg", 2)}

	var order []string
	for q.hasWorkLocked() {
		order = append(order, q.popLocked().entry.probe.Proxy.Name())
	}
	require.Equal(t, []string{"fa0", "fb0", "fa1", "fb1", "bg0", "bg1"}, order)
}

// A probe a dial is waiting on overtakes the scheduled work.
func TestPopLockedPriorityFirst(t *testing.T) {
	q := &queue{limit: 1}
	normal := &batch{q: q, probes: []*probeEntry{{probe: Probe{Proxy: newTestProxy("n0", okProbe(1))}}}}
	urgent := &batch{q: q}
	q.ring = []*batch{normal}
	q.priority = []probeItem{{batch: urgent, entry: &probeEntry{probe: Probe{Proxy: newTestProxy("p0", okProbe(1))}}}}

	require.Equal(t, "p0", q.popLocked().entry.probe.Proxy.Name())
	require.Equal(t, "n0", q.popLocked().entry.probe.Proxy.Name())
	require.False(t, q.hasWorkLocked())
}

// A quick check submitted during a big sweep reaches the slots before the
// sweep ends.
func TestQueueInterleavesChecks(t *testing.T) {
	q := newQueue(1)
	recorder := &startRecorder{}
	gate := make(chan struct{})

	big := make([]Probe, 0, 6)
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("big%d", i)
		big = append(big, Probe{
			Proxy: newTestProxy(name, func(context.Context) (uint16, error) {
				recorder.add(name)
				if name == "big0" {
					<-gate
				}
				return 10, nil
			}),
			Timeout: time.Second,
		})
	}
	small := make([]Probe, 0, 2)
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("small%d", i)
		small = append(small, Probe{
			Proxy: newTestProxy(name, func(context.Context) (uint16, error) {
				recorder.add(name)
				return 10, nil
			}),
			Timeout: time.Second,
		})
	}

	bigCheck := q.submit("big", false, big, nil)
	smallCheck := q.submit("small", false, small, nil)
	close(gate)

	require.True(t, bigCheck.Wait(context.Background()))
	require.True(t, smallCheck.Wait(context.Background()))

	order := recorder.list()
	require.Len(t, order, 8)
	require.Less(t, recorder.index("small0"), recorder.index("big5"),
		"the quick check must not wait for the whole sweep ahead of it")
}

// A check of a group that does not wait for traffic reaches the slots before
// the background sweep, even when the sweep was submitted first.
func TestQueueForegroundOvertakesSweep(t *testing.T) {
	q := newQueue(1)
	recorder := &startRecorder{}
	gate := make(chan struct{})

	big := make([]Probe, 0, 6)
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("big%d", i)
		big = append(big, Probe{
			Proxy: newTestProxy(name, func(context.Context) (uint16, error) {
				recorder.add(name)
				if name == "big0" {
					<-gate
				}
				return 10, nil
			}),
			Timeout: time.Second,
		})
	}
	foreground := make([]Probe, 0, 2)
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("fg%d", i)
		foreground = append(foreground, Probe{
			Proxy: newTestProxy(name, func(context.Context) (uint16, error) {
				recorder.add(name)
				return 10, nil
			}),
			Timeout: time.Second,
		})
	}

	bigCheck := q.submit("sweep", false, big, nil)
	requireEventually(t, func() bool { return recorder.index("big0") == 0 }, "the sweep must own the only slot")

	fgCheck := q.submit("foreground", true, foreground, nil)
	close(gate)

	require.True(t, bigCheck.Wait(context.Background()))
	require.True(t, fgCheck.Wait(context.Background()))

	order := recorder.list()
	require.Len(t, order, 8)
	require.Less(t, recorder.index("fg0"), recorder.index("big1"),
		"the foreground check must not wait for the whole sweep")
}

// A priority batch goes first as soon as a slot frees up.
func TestQueuePriorityOvertakesRing(t *testing.T) {
	q := newQueue(1)
	recorder := &startRecorder{}
	gate := make(chan struct{})

	normal := make([]Probe, 0, 3)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("n%d", i)
		normal = append(normal, Probe{
			Proxy: newTestProxy(name, func(context.Context) (uint16, error) {
				recorder.add(name)
				if name == "n0" {
					<-gate
				}
				return 10, nil
			}),
			Timeout: time.Second,
		})
	}

	normalCheck := q.submit("sweep", false, normal, nil)

	// The sweep must own the slot before the urgent batch arrives, otherwise
	// the priority probe is simply the first queued probe.
	requireEventually(t, func() bool { return recorder.index("n0") == 0 }, "the first sweep probe must hold the slot")

	urgentCheck := q.submit("recovery", false, []Probe{{
		Proxy: newTestProxy("p0", func(context.Context) (uint16, error) {
			recorder.add("p0")
			return 5, nil
		}),
		Timeout:  time.Second,
		Priority: true,
	}}, nil)
	close(gate)

	require.True(t, normalCheck.Wait(context.Background()))
	require.True(t, urgentCheck.Wait(context.Background()))
	require.Equal(t, []string{"n0", "p0", "n1", "n2"}, recorder.list())
}

// A probe that runs out of its own timeout is a dead node: its error is the
// verdict the group stores, not a cancelled measurement.
func TestQueueNodeTimeoutIsAVerdict(t *testing.T) {
	q := newQueue(2)

	var result Result
	check := q.submit("sweep", false, []Probe{{
		Proxy: newTestProxy("hanging", func(ctx context.Context) (uint16, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}),
		Timeout: 20 * time.Millisecond,
	}}, func(r Result) { result = r })

	start := time.Now()
	require.True(t, check.Wait(context.Background()))
	require.Less(t, time.Since(start), time.Second)
	require.Error(t, result.Err)
	require.ErrorIs(t, result.Err, context.DeadlineExceeded)
}

// The caller's budget bounds only the answer: the probes keep running to their
// own timeout and still report.
func TestQueueWaitDoesNotCutProbes(t *testing.T) {
	q := newQueue(2)
	results := make(chan Result, 2)

	check := q.submit("sweep", false, []Probe{
		{
			Proxy:   newTestProxy("fast", okProbe(5)),
			Timeout: time.Second,
		},
		{
			Proxy: newTestProxy("slow", func(context.Context) (uint16, error) {
				time.Sleep(80 * time.Millisecond)
				return 42, nil
			}),
			Timeout: time.Second,
		},
	}, func(r Result) { results <- r })

	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.False(t, check.Wait(shortCtx), "the answer must not wait for the slow probe")

	require.True(t, check.Wait(context.Background()))
	close(results)

	delays := make(map[string]uint16)
	for result := range results {
		require.NoError(t, result.Err)
		delays[result.Name] = result.Delay
	}
	require.Equal(t, map[string]uint16{"fast": 5, "slow": 42}, delays,
		"a probe must run to its own timeout after the caller is gone")
}

// The limit is applied at runtime: raising it starts the queued probes without
// a restart.
func TestSetConcurrency(t *testing.T) {
	q := newQueue(1)
	gate := make(chan struct{})
	var inFlight atomic.Int32

	probes := make([]Probe, 0, 3)
	for i := 0; i < 3; i++ {
		probes = append(probes, Probe{
			Proxy: newTestProxy(fmt.Sprintf("p%d", i), func(context.Context) (uint16, error) {
				inFlight.Add(1)
				<-gate
				inFlight.Add(-1)
				return 10, nil
			}),
			Timeout: time.Second,
		})
	}

	check := q.submit("sweep", false, probes, nil)
	requireEventually(t, func() bool { return inFlight.Load() == 1 }, "the first probe must start")

	time.Sleep(30 * time.Millisecond)
	require.Equal(t, int32(1), inFlight.Load())

	q.setConcurrency(3)
	requireEventually(t, func() bool { return inFlight.Load() == 3 }, "the raised limit must start the queued probes")

	close(gate)
	require.True(t, check.Wait(context.Background()))
}

// A group must not hold a slot while it waits for the probes it submits itself:
// it runs on the caller's goroutine, even when every slot is busy.
func TestQueueRunsGroupProbesInline(t *testing.T) {
	q := newQueue(1)
	gate := make(chan struct{})
	groupStarted := make(chan struct{}, 1)

	groupProxy := &fakeProxy{
		name:    "group",
		adapter: &fakeGroupAdapter{},
		probe: func(context.Context) (uint16, error) {
			groupStarted <- struct{}{}
			return 7, nil
		},
	}
	normalProxy := newTestProxy("normal", func(context.Context) (uint16, error) {
		<-gate
		return 10, nil
	})

	check := q.submit("mixed", false, []Probe{
		{Proxy: normalProxy, Timeout: time.Second},
		{Proxy: groupProxy, Timeout: time.Second},
	}, nil)

	select {
	case <-groupStarted:
	case <-time.After(time.Second):
		t.Fatal("the group probe must run without waiting for a slot")
	}
	close(gate)
	require.True(t, check.Wait(context.Background()))
}

func TestQueueEmptySubmit(t *testing.T) {
	q := newQueue(1)
	check := q.submit("empty", false, nil, func(Result) {
		t.Fatal("an empty check must not report")
	})
	require.True(t, check.Wait(context.Background()))
}

// Two checks that ask the same question share one probe: the second check
// waits on the measurement already in flight instead of loading the path again.
func TestSubmitSharesInFlightProbe(t *testing.T) {
	q := newQueue(4)
	gate := make(chan struct{})
	var calls atomic.Int32

	proxy := newTestProxy("shared", func(context.Context) (uint16, error) {
		calls.Add(1)
		<-gate
		return 40, nil
	})
	probe := Probe{Proxy: proxy, URL: "https://example.com/204", Timeout: time.Second}

	results := make(chan Result, 2)
	first := q.submit("first", false, []Probe{probe}, func(r Result) { results <- r })
	requireEventually(t, func() bool { return calls.Load() == 1 }, "the first probe must hold the measurement")

	second := q.submit("second", false, []Probe{probe}, func(r Result) { results <- r })
	close(gate)

	require.True(t, first.Wait(context.Background()))
	require.True(t, second.Wait(context.Background()))
	require.Equal(t, int32(1), calls.Load(), "the second check must join the probe in flight")

	close(results)
	for result := range results {
		require.NoError(t, result.Err)
		require.Equal(t, uint16(40), result.Delay)
	}
}

// A single-node test with the group's budget asks the same question the sweep
// asks, so it joins the probe in flight instead of measuring the node again.
func TestTestProxyWithBudgetSharesInFlightProbe(t *testing.T) {
	gate := make(chan struct{})
	var calls atomic.Int32
	proxy := newTestProxy("shared", func(context.Context) (uint16, error) {
		calls.Add(1)
		<-gate
		return 40, nil
	})

	type answer struct {
		delay uint16
		err   error
	}
	test := func() <-chan answer {
		out := make(chan answer, 1)
		go func() {
			delay, err := TestProxyWithBudget(context.Background(), proxy, "https://example.com/204", nil, time.Second)
			out <- answer{delay: delay, err: err}
		}()
		return out
	}

	first := test()
	requireEventually(t, func() bool { return calls.Load() == 1 }, "the first test must hold the measurement")
	second := test()

	// The goroutine that runs the second test submits asynchronously, so wait
	// until it has actually joined the running measurement before letting the
	// first one finish: closing the gate earlier would race the submit.
	key, keyed := probeKeyOf(Probe{Proxy: proxy, URL: "https://example.com/204", Timeout: time.Second, Priority: true})
	require.True(t, keyed)
	requireEventually(t, func() bool {
		defaultQueue.mu.Lock()
		defer defaultQueue.mu.Unlock()
		entry, ok := defaultQueue.inflight[key]
		return ok && entry.running && len(entry.waiters) == 2
	}, "the second test must join the probe in flight")
	close(gate)

	for _, result := range []answer{<-first, <-second} {
		require.NoError(t, result.err)
		require.Equal(t, uint16(40), result.delay)
	}
	require.Equal(t, int32(1), calls.Load(), "the second test must share the measurement")
}

// The same proxy listed twice in one check is still one measurement, and both
// entries of the check report.
func TestSubmitSharesDuplicateProbesInBatch(t *testing.T) {
	q := newQueue(2)
	var calls atomic.Int32
	var delivered atomic.Int32

	probe := Probe{
		Proxy: newTestProxy("dup", func(context.Context) (uint16, error) {
			calls.Add(1)
			return 30, nil
		}),
		URL:     "https://example.com/204",
		Timeout: time.Second,
	}

	check := q.submit("batch", false, []Probe{probe, probe}, func(Result) { delivered.Add(1) })
	require.True(t, check.Wait(context.Background()))
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, int32(2), delivered.Load())
}

// Two proxies that share a name are still two different nodes: their probes
// must not be shared.
func TestSubmitKeepsEqualNamedProxiesApart(t *testing.T) {
	q := newQueue(2)
	var calls atomic.Int32

	newProxy := func() *fakeProxy {
		return newTestProxy("same-name", func(context.Context) (uint16, error) {
			calls.Add(1)
			return 10, nil
		})
	}

	check := q.submit("batch", false, []Probe{
		{Proxy: newProxy(), URL: "https://example.com/204", Timeout: time.Second},
		{Proxy: newProxy(), URL: "https://example.com/204", Timeout: time.Second},
	}, nil)
	require.True(t, check.Wait(context.Background()))
	require.Equal(t, int32(2), calls.Load(), "only the same proxy may share a probe")
}

// A priority probe does not join a twin that still waits in the ring: the dial
// would wait behind the whole sweep, which is what the priority lane exists to
// prevent.
func TestPriorityProbeDoesNotJoinQueuedTwin(t *testing.T) {
	q := newQueue(1)
	gate := make(chan struct{})

	blocker := newTestProxy("blocker", func(context.Context) (uint16, error) {
		<-gate
		return 1, nil
	})
	twin := newTestProxy("twin", okProbe(5))

	block := q.submit("block", false, []Probe{{Proxy: blocker, Timeout: time.Second}}, nil)
	requireEventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.running == 1
	}, "the blocker must own the only slot")

	twinProbe := Probe{Proxy: twin, URL: "https://example.com/204", Timeout: time.Second}
	sweep := q.submit("sweep", false, []Probe{twinProbe}, nil)

	urgentProbe := twinProbe
	urgentProbe.Priority = true
	recovery := q.submit("recovery", false, []Probe{urgentProbe}, nil)

	q.mu.Lock()
	priorityQueued := len(q.priority)
	q.mu.Unlock()
	require.Equal(t, 1, priorityQueued, "the urgent twin must go to the priority lane, not join the ring")

	close(gate)
	require.True(t, block.Wait(context.Background()))
	require.True(t, sweep.Wait(context.Background()))
	require.True(t, recovery.Wait(context.Background()))
}

func TestSetConcurrencyClamps(t *testing.T) {
	original := GetConcurrency()
	t.Cleanup(func() { SetConcurrency(original) })

	SetConcurrency(0)
	require.Equal(t, DefaultConcurrency, GetConcurrency())

	SetConcurrency(maxConcurrency + 10)
	require.Equal(t, maxConcurrency, GetConcurrency())
}

// The urgent ring serves the checks of the groups that have nothing to route
// by before both warm lanes, and keeps its own round-robin.
func TestPopLockedUrgentBeforeWarmLanes(t *testing.T) {
	q := &queue{limit: 1}

	newBatch := func(prefix string, count int) *batch {
		probes := make([]*probeEntry, 0, count)
		for i := 0; i < count; i++ {
			probes = append(probes, &probeEntry{probe: Probe{Proxy: newTestProxy(fmt.Sprintf("%s%d", prefix, i), okProbe(1))}})
		}
		return &batch{q: q, probes: probes}
	}

	q.urgent = []*batch{newBatch("ua", 2), newBatch("ub", 2)}
	q.foreground = []*batch{newBatch("fa", 2)}
	q.ring = []*batch{newBatch("bg", 2)}

	var order []string
	for q.hasWorkLocked() {
		order = append(order, q.popLocked().entry.probe.Proxy.Name())
	}
	require.Equal(t, []string{"ua0", "ub0", "ua1", "ub1", "fa0", "fa1", "bg0", "bg1"}, order,
		"the urgent ring must be served before the warm lanes and keep its own round-robin")
}

// A cold check does not join a twin that still waits in the ring: it would
// wait behind the whole sweep, which is what the urgent lane exists to prevent.
// The map keeps the more urgent measurement, so the next cold ask joins it.
func TestUrgentProbeDoesNotJoinQueuedRingTwin(t *testing.T) {
	q := newQueue(1)
	gate := make(chan struct{})

	blocker := newTestProxy("blocker", func(context.Context) (uint16, error) {
		<-gate
		return 1, nil
	})
	var calls atomic.Int32
	twin := newTestProxy("twin", func(context.Context) (uint16, error) {
		calls.Add(1)
		return 5, nil
	})

	block := q.submit("block", false, []Probe{{Proxy: blocker, Timeout: time.Second}}, nil)
	requireEventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.running == 1
	}, "the blocker must own the only slot")

	probe := Probe{Proxy: twin, URL: "https://example.com/204", Timeout: time.Second}
	sweep := q.submit("sweep", false, []Probe{probe}, nil)
	cold := q.submitInLane("cold", laneUrgent, laneForeground, []Probe{probe}, nil)

	key, keyed := probeKeyOf(probe)
	require.True(t, keyed)
	q.mu.Lock()
	urgentQueued := len(q.urgent)
	ringQueued := len(q.ring)
	entryLane := q.inflight[key].lane
	q.mu.Unlock()
	require.Equal(t, 1, urgentQueued, "the cold check must not wait behind the queued ring twin")
	require.Equal(t, 1, ringQueued, "the sweep keeps its own entry")
	require.Equal(t, laneUrgent, entryLane, "the map must keep the more urgent measurement of the key")

	close(gate)
	require.True(t, block.Wait(context.Background()))
	require.True(t, sweep.Wait(context.Background()))
	require.True(t, cold.Wait(context.Background()))
	require.Equal(t, int32(2), calls.Load(),
		"the twin must be measured twice when the queued ring entry could not be shared")
}

// Two cold checks of the same measurement are one question: the second joins
// the entry already queued in the urgent lane.
func TestUrgentProbeJoinsQueuedUrgentTwin(t *testing.T) {
	q := newQueue(1)
	gate := make(chan struct{})

	blocker := newTestProxy("blocker", func(context.Context) (uint16, error) {
		<-gate
		return 1, nil
	})
	var calls atomic.Int32
	twin := newTestProxy("twin", func(context.Context) (uint16, error) {
		calls.Add(1)
		return 5, nil
	})

	block := q.submit("block", false, []Probe{{Proxy: blocker, Timeout: time.Second}}, nil)
	requireEventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.running == 1
	}, "the blocker must own the only slot")

	probe := Probe{Proxy: twin, URL: "https://example.com/204", Timeout: time.Second}
	first := q.submitInLane("cold", laneUrgent, laneForeground, []Probe{probe}, nil)
	second := q.submitInLane("cold", laneUrgent, laneForeground, []Probe{probe}, nil)

	key, keyed := probeKeyOf(probe)
	require.True(t, keyed)
	q.mu.Lock()
	queued := len(q.urgent)
	waiters := len(q.inflight[key].waiters)
	q.mu.Unlock()
	require.Equal(t, 1, queued, "the second cold check must join the queued urgent probe")
	require.Equal(t, 2, waiters)

	close(gate)
	require.True(t, block.Wait(context.Background()))
	require.True(t, first.Wait(context.Background()))
	require.True(t, second.Wait(context.Background()))
	require.Equal(t, int32(1), calls.Load(), "the member must be probed once")
}

// The first alive result takes the rest of a cold check out of the urgent
// ring: the pool found a member, so the remaining probes are ordinary work and
// must stop delaying the groups that are still looking. The entries keep the
// lane they dropped to, so the next cold ask does not join them.
func TestUrgentCheckDemotesAfterFirstAliveResult(t *testing.T) {
	for _, tc := range []struct {
		name string
		rest lane
		home func(q *queue) []*batch
	}{
		{name: "foreground", rest: laneForeground, home: func(q *queue) []*batch { return q.foreground }},
		{name: "ring", rest: laneRing, home: func(q *queue) []*batch { return q.ring }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := newQueue(1)
			firstStarted := make(chan struct{})
			releaseFirst := make(chan struct{})
			secondStarted := make(chan struct{})
			releaseSecond := make(chan struct{})

			probe := func(name string, run func(context.Context) (uint16, error)) Probe {
				return Probe{Proxy: newTestProxy(name, run), URL: "https://example.com/204", Timeout: time.Second}
			}
			probes := []Probe{
				probe("p0", func(context.Context) (uint16, error) {
					close(firstStarted)
					<-releaseFirst
					return 10, nil
				}),
				probe("p1", func(context.Context) (uint16, error) {
					close(secondStarted)
					<-releaseSecond
					return 10, nil
				}),
				probe("p2", okProbe(10)),
				probe("p3", okProbe(10)),
			}

			check := q.submitInLane("cold", laneUrgent, tc.rest, probes, nil)
			<-firstStarted

			q.mu.Lock()
			urgentBefore := len(q.urgent)
			q.mu.Unlock()
			require.Equal(t, 1, urgentBefore, "the cold check must start in the urgent ring")

			close(releaseFirst)
			<-secondStarted

			requireEventually(t, func() bool {
				q.mu.Lock()
				defer q.mu.Unlock()
				return len(q.urgent) == 0
			}, "the first alive result must take the rest of the check out of the urgent ring")

			q.mu.Lock()
			home := tc.home(q)
			homeLen := len(home)
			var homeLane lane
			var nextLane lane
			if homeLen > 0 {
				homeLane = home[0].lane
				if next := home[0].next; next < len(home[0].probes) {
					nextLane = home[0].probes[next].lane
				}
			}
			q.mu.Unlock()
			require.Equal(t, 1, homeLen, "the rest of the check must wait in its home lane")
			require.Equal(t, tc.rest, homeLane)
			require.Equal(t, tc.rest, nextLane, "a queued entry must keep the lane it dropped to")

			close(releaseSecond)
			require.True(t, check.Wait(context.Background()))
		})
	}
}

// A profile with several cold groups at once: the urgent ring hands out one
// probe per group in turn, so every cold group starts looking for a working
// member in the first wave instead of waiting for the group ahead of it to
// finish. A group that finds a member drops its remaining probes to its home
// lane, and a group that wakes up later still takes the urgent lane ahead of
// those leftovers. A probe that already waits in the urgent lane under the
// same key is joined instead (see TestUrgentProbeJoinsQueuedUrgentTwin),
// which is the only case where a late group does not stand in the ring itself.
func TestColdGroupsShareTheFirstWaveAndLateGroupStaysUrgent(t *testing.T) {
	q := &queue{limit: 1}

	newBatch := func(prefix string, count int, rest lane) *batch {
		probes := make([]*probeEntry, 0, count)
		for i := 0; i < count; i++ {
			probes = append(probes, &probeEntry{probe: Probe{Proxy: newTestProxy(fmt.Sprintf("%s%d", prefix, i), okProbe(1))}})
		}
		return &batch{q: q, probes: probes, lane: laneUrgent, rest: rest}
	}
	pop := func() string { return q.popLocked().entry.probe.Proxy.Name() }

	first := newBatch("g1-", 4, laneForeground)  // lazy: false
	second := newBatch("g2-", 4, laneForeground) // lazy: false
	third := newBatch("g3-", 4, laneForeground)  // lazy: false
	q.urgent = []*batch{first, second, third}

	require.Equal(t, []string{"g1-0", "g2-0", "g3-0", "g1-1"},
		[]string{pop(), pop(), pop(), pop()},
		"the first wave must give each cold group a slot, not sweep the first group whole")

	// The first group has measured a member: its remaining probes are ordinary
	// foreground work, and the groups still looking keep the urgent ring.
	q.demoteLocked(first)
	require.Equal(t, []string{"g2-1", "g3-1", "g2-2", "g3-2"},
		[]string{pop(), pop(), pop(), pop()},
		"the leftovers of a group that found a member must not delay the groups still looking")

	// A lazy group joins while the sweep is running: it is cold too, and where
	// the urgent cursor stands decides whether it jumps ahead of the round or
	// waits for the turn after it. Either way both of its probes are served
	// before the foreground leftovers, so a group the user only reached later
	// still gets its working member without waiting for the whole sweep.
	fourth := newBatch("g4-", 2, laneRing)
	q.urgent = append(q.urgent, fourth)
	require.Equal(t, []string{"g4-0", "g2-3", "g3-3", "g4-1", "g1-2", "g1-3"},
		[]string{pop(), pop(), pop(), pop(), pop(), pop()},
		"a cold group that starts later must still be served before the leftovers")
}

// Two priority probes of the same measurement are one question: the second
// caller waits on the entry already queued in the priority lane instead of
// loading the member a second time.
func TestPriorityProbeJoinsQueuedPriorityTwin(t *testing.T) {
	q := newQueue(1)
	gate := make(chan struct{})

	blocker := newTestProxy("blocker", func(context.Context) (uint16, error) {
		<-gate
		return 1, nil
	})
	var calls atomic.Int32
	twin := newTestProxy("twin", func(context.Context) (uint16, error) {
		calls.Add(1)
		return 5, nil
	})

	block := q.submit("block", false, []Probe{{Proxy: blocker, Timeout: time.Second}}, nil)
	requireEventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.running == 1
	}, "the blocker must own the only slot")

	twinProbe := Probe{Proxy: twin, URL: "https://example.com/204", Timeout: time.Second, Priority: true}
	first := q.submit("recovery", false, []Probe{twinProbe}, nil)
	second := q.submit("recovery", false, []Probe{twinProbe}, nil)

	q.mu.Lock()
	priorityQueued := len(q.priority)
	q.mu.Unlock()
	require.Equal(t, 1, priorityQueued, "the second dial must join the queued priority probe")

	close(gate)
	require.True(t, block.Wait(context.Background()))
	require.True(t, first.Wait(context.Background()))
	require.True(t, second.Wait(context.Background()))
	require.Equal(t, int32(1), calls.Load(), "the member must be probed once")
}

// newIdleQueue builds a queue without its dispatcher: the test decides when a
// probe is taken, so the state of the lanes can be inspected.
func newIdleQueue() *queue {
	q := &queue{limit: 1, inflight: make(map[probeKey]*probeEntry)}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// chainProbes builds the probes of a check named prefix0, prefix1, ...
func chainProbes(prefix string, count int) []Probe {
	probes := make([]Probe, 0, count)
	for i := 0; i < count; i++ {
		probes = append(probes, Probe{Proxy: newTestProxy(fmt.Sprintf("%s%d", prefix, i), okProbe(1))})
	}
	return probes
}

// chainBatch builds a chained batch that goes straight into a lane of the test
// queue, without the registry deciding its place.
func chainBatch(q *queue, prefix string, count int, place chainPlace) *batch {
	probes := make([]*probeEntry, 0, count)
	for i := 0; i < count; i++ {
		probes = append(probes, &probeEntry{probe: Probe{Proxy: newTestProxy(fmt.Sprintf("%s%d", prefix, i), okProbe(1))}})
	}
	return &batch{q: q, source: prefix, probes: probes, lane: laneChain, rest: laneRing, place: place}
}

// The ordered chain serves one wave of the profile at a time: the explicit
// numbers ascending, and the pools that set none after them in declaration
// order. Equal numbers are one wave and are served round-robin, so the groups
// the profile put together look for a member together.
func TestPopLockedChainServesWavesInOrder(t *testing.T) {
	q := newIdleQueue()

	q.insertChainLocked(chainBatch(q, "d", 2, chainPlace{declared: 3}))
	q.insertChainLocked(chainBatch(q, "a", 2, chainPlace{explicit: true, value: 1}))
	q.insertChainLocked(chainBatch(q, "c", 2, chainPlace{explicit: true, value: 2}))
	q.insertChainLocked(chainBatch(q, "b", 2, chainPlace{explicit: true, value: 1}))
	q.insertChainLocked(chainBatch(q, "e", 2, chainPlace{declared: 4}))

	var order []string
	for q.hasWorkLocked() {
		order = append(order, q.popLocked().entry.probe.Proxy.Name())
	}
	require.Equal(t, []string{"a0", "b0", "a1", "b1", "c0", "c1", "d0", "d1", "e0", "e1"}, order,
		"the waves must go in profile order, one at a time, round-robin inside a wave")
}

// A check a dial waits on goes ahead of the waves still ahead of it: the dial
// is the reason the check runs at all.
func TestPopLockedChainPromotedFirst(t *testing.T) {
	q := newIdleQueue()
	q.insertChainLocked(chainBatch(q, "b", 2, chainPlace{explicit: true, value: 2}))
	q.insertChainLocked(chainBatch(q, "a", 2, chainPlace{explicit: true, value: 1}))

	require.Equal(t, "a", q.chainHeadLocked().source)
	q.promote("b")
	require.Equal(t, "b", q.chainHeadLocked().source,
		"a dial-waiting check must go ahead of the wave the profile put before it")

	var order []string
	for q.hasWorkLocked() {
		order = append(order, q.popLocked().entry.probe.Proxy.Name())
	}
	require.Equal(t, []string{"b0", "b1", "a0", "a1"}, order)
}

// The registry follows the profile: explicit numbers first, ascending, and the
// pools without a number after them in declaration order. A source the profile
// did not register (a host-built group, a test) takes the tail.
func TestChainRegistryOrdersNumbersThenDeclaration(t *testing.T) {
	one, two := 1, 2
	BeginChain()
	RegisterChain("plain-a", nil, 0)
	RegisterChain("two", &two, 1)
	RegisterChain("one-a", &one, 4)
	RegisterChain("one-b", &one, 2)
	CommitChain()
	defer ResetChain()

	require.True(t, chainPlaceOf("one-a").sameWave(chainPlaceOf("one-b")),
		"equal numbers must share a wave")
	require.False(t, chainPlaceOf("two").sameWave(chainPlaceOf("one-a")))
	require.True(t, chainPlaceOf("one-a").before(chainPlaceOf("two")))
	require.True(t, chainPlaceOf("two").before(chainPlaceOf("plain-a")),
		"a pool without a number must follow every explicit one, whatever its declaration index")
	require.True(t, chainPlaceOf("plain-a").before(chainPlaceOf("unregistered")),
		"a source the profile did not register must stay at the tail")
}

// Begin/commit is two-phase: a profile that fails to parse must not take away
// the order the running profile uses.
func TestCommitChainKeepsTheRunningOrderOnFailedParse(t *testing.T) {
	one := 1
	BeginChain()
	RegisterChain("kept", &one, 0)
	CommitChain()
	defer ResetChain()

	// the next profile starts building its own order and fails before Commit
	BeginChain()
	RegisterChain("dropped", nil, 0)

	place := chainPlaceOf("kept")
	require.True(t, place.explicit)
	require.Equal(t, 1, place.value)
	require.False(t, chainPlaceOf("dropped").explicit,
		"an uncommitted registry must not be visible to the queue")
}

// The first alive member takes the rest of the wave out of the chain: the
// groups of the wave have something to route by, and the next wave must not
// wait for their remaining probes. A wave is the equal number, not one batch:
// the other group of the wave drops along.
func TestChainDemotesTheWholeWaveAfterFirstAlive(t *testing.T) {
	one := 1
	BeginChain()
	RegisterChain("a", &one, 0)
	RegisterChain("b", &one, 1)
	RegisterChain("c", nil, 2)
	CommitChain()
	defer ResetChain()

	q := newIdleQueue()
	q.submitChain("a", false, chainProbes("a", 3), nil)
	q.submitChain("b", false, chainProbes("b", 3), nil)
	q.submitChain("c", false, chainProbes("c", 3), nil)

	find := func(source string) *batch {
		for _, b := range q.chain {
			if b.source == source {
				return b
			}
		}
		return nil
	}

	q.mu.Lock()
	first := find("a")
	require.NotNil(t, first)
	require.Same(t, first, q.chainHeadLocked(), "the wave of the lowest number must be served first")

	q.demoteChainWaveLocked(first)

	require.Nil(t, find("a"))
	require.Nil(t, find("b"), "the rest of the wave must leave the chain with the group that answered")
	require.NotNil(t, find("c"), "the next wave must keep its place after one wave found a member")

	require.Len(t, q.ring, 2, "the rest of the wave must drop to its home lane")
	for _, b := range q.ring {
		require.Equal(t, laneRing, b.lane)
		for _, entry := range b.probes[b.next:] {
			require.Equal(t, laneRing, entry.lane, "a queued entry must keep the lane it dropped to")
		}
	}
	q.mu.Unlock()
}

// Switching `probe-order` to parallel must not strand a check in the chain
// lane: every queued check moves to the urgent ring the parallel order serves.
func TestDrainChainMovesChecksToTheUrgentRing(t *testing.T) {
	q := newIdleQueue()
	BeginChain()
	RegisterChain("a", nil, 0)
	RegisterChain("b", nil, 1)
	CommitChain()
	defer ResetChain()

	q.submitChain("a", true, chainProbes("a", 2), nil)
	q.submitChain("b", false, chainProbes("b", 2), nil)

	q.drainChain()

	q.mu.Lock()
	defer q.mu.Unlock()
	require.Empty(t, q.chain)
	require.Len(t, q.urgent, 2)
	for _, b := range q.urgent {
		require.Equal(t, laneUrgent, b.lane)
		for _, entry := range b.probes[b.next:] {
			require.Equal(t, laneUrgent, entry.lane)
		}
	}
	require.Equal(t, laneForeground, q.urgent[0].rest, "a foreground check must keep the lane it drops to")
}

// A dial can win the race against its own check: the promotion must be
// remembered until the check reaches the queue, and only one check takes it.
func TestPromoteBeforeSubmitReachesTheCheck(t *testing.T) {
	q := newIdleQueue()
	BeginChain()
	RegisterChain("first", nil, 0)
	RegisterChain("late", nil, 5)
	CommitChain()
	defer ResetChain()

	q.promote("late")
	q.submitChain("first", false, chainProbes("first", 2), nil)
	q.submitChain("late", false, chainProbes("late", 2), nil)

	q.mu.Lock()
	defer q.mu.Unlock()
	require.Equal(t, "late", q.chainHeadLocked().source,
		"a promotion that arrived before the check must still put it at the head")
	require.Empty(t, q.promoted, "the request must be consumed by the check it promoted")
}

// `probe-order: parallel` puts a cold check back in the urgent lane, and a
// check that already waits in the chain moves along with it.
func TestSubmitChainFollowsProbeOrder(t *testing.T) {
	originalOrder := GetProbeOrder()
	originalLimit := GetConcurrency()
	SetConcurrency(1)
	SetProbeOrder(OrderSequential)
	t.Cleanup(func() {
		SetProbeOrder(originalOrder)
		SetConcurrency(originalLimit)
	})

	started := make(chan struct{})
	gate := make(chan struct{})
	block := Submit("block", []Probe{{Proxy: newTestProxy("slot", func(context.Context) (uint16, error) {
		close(started)
		<-gate
		return 10, nil
	}), Timeout: time.Second}}, nil)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the blocker must own the only slot")
	}

	probe := Probe{Proxy: newTestProxy("chained", okProbe(5)), URL: "https://example.com/204", Timeout: time.Second}
	chained := SubmitChain("chained", false, []Probe{probe}, nil)

	defaultQueue.mu.Lock()
	chainQueued := len(defaultQueue.chain)
	defaultQueue.mu.Unlock()
	require.Equal(t, 1, chainQueued, "a sequential profile must run the cold check in the chain")

	SetProbeOrder(OrderParallel)

	defaultQueue.mu.Lock()
	chainAfter := len(defaultQueue.chain)
	urgentQueued := len(defaultQueue.urgent)
	defaultQueue.mu.Unlock()
	require.Zero(t, chainAfter)
	require.Equal(t, 1, urgentQueued, "switching to parallel must not strand the chained check")

	close(gate)
	require.True(t, block.Wait(context.Background()))
	require.True(t, chained.Wait(context.Background()))
}

func TestParseProbeOrder(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    ProbeOrder
		wantErr bool
	}{
		{value: "", want: OrderSequential},
		{value: "sequential", want: OrderSequential},
		{value: "SEQUENTIAL", want: OrderSequential},
		{value: "parallel", want: OrderParallel},
		{value: " Parallel ", want: OrderParallel},
		{value: "ordered", wantErr: true},
	} {
		got, err := ParseProbeOrder(tc.value)
		if tc.wantErr {
			require.Error(t, err, tc.value)
			continue
		}
		require.NoError(t, err, tc.value)
		require.Equal(t, tc.want, got, tc.value)
	}
	require.Equal(t, "sequential", OrderSequential.String())
	require.Equal(t, "parallel", OrderParallel.String())
}
