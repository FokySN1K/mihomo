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

	check := q.submit("sweep", probes, func(Result) { delivered.Add(1) })
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
		probes := make([]Probe, 0, count)
		for i := 0; i < count; i++ {
			probes = append(probes, Probe{Proxy: newTestProxy(fmt.Sprintf("%s%d", prefix, i), okProbe(1))})
		}
		return &batch{q: q, probes: probes}
	}

	q.ring = []*batch{newBatch(q, "a", 3), newBatch(q, "b", 3)}

	var order []string
	for q.hasWorkLocked() {
		order = append(order, q.popLocked().probe.Proxy.Name())
	}
	require.Equal(t, []string{"a0", "b0", "a1", "b1", "a2", "b2"}, order)
}

// A probe a dial is waiting on overtakes the scheduled work.
func TestPopLockedPriorityFirst(t *testing.T) {
	q := &queue{limit: 1}
	normal := &batch{q: q, probes: []Probe{{Proxy: newTestProxy("n0", okProbe(1))}}}
	urgent := &batch{q: q}
	q.ring = []*batch{normal}
	q.priority = []probeItem{{batch: urgent, probe: Probe{Proxy: newTestProxy("p0", okProbe(1))}}}

	require.Equal(t, "p0", q.popLocked().probe.Proxy.Name())
	require.Equal(t, "n0", q.popLocked().probe.Proxy.Name())
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

	bigCheck := q.submit("big", big, nil)
	smallCheck := q.submit("small", small, nil)
	close(gate)

	require.True(t, bigCheck.Wait(context.Background()))
	require.True(t, smallCheck.Wait(context.Background()))

	order := recorder.list()
	require.Len(t, order, 8)
	require.Less(t, recorder.index("small0"), recorder.index("big5"),
		"the quick check must not wait for the whole sweep ahead of it")
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

	normalCheck := q.submit("sweep", normal, nil)

	// The sweep must own the slot before the urgent batch arrives, otherwise
	// the priority probe is simply the first queued probe.
	requireEventually(t, func() bool { return recorder.index("n0") == 0 }, "the first sweep probe must hold the slot")

	urgentCheck := q.submit("recovery", []Probe{{
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
	check := q.submit("sweep", []Probe{{
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

	check := q.submit("sweep", []Probe{
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

	check := q.submit("sweep", probes, nil)
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

	check := q.submit("mixed", []Probe{
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
	check := q.submit("empty", nil, func(Result) {
		t.Fatal("an empty check must not report")
	})
	require.True(t, check.Wait(context.Background()))
}

func TestSetConcurrencyClamps(t *testing.T) {
	original := GetConcurrency()
	t.Cleanup(func() { SetConcurrency(original) })

	SetConcurrency(0)
	require.Equal(t, DefaultConcurrency, GetConcurrency())

	SetConcurrency(maxConcurrency + 10)
	require.Equal(t, maxConcurrency, GetConcurrency())
}
