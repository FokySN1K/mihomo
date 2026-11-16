package outboundgroup

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outboundgroup/probequeue"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

// groupbaseLaneProxy records where its probe started, so a test can read the
// order the shared queue served two groups in.
type groupbaseLaneProxy struct {
	*poolOrderedProxy
	log *dialLog
}

func (p *groupbaseLaneProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	p.log.record(p.Name())
	return 50, nil
}

// groupbaseBlocker holds the only queue slot until the test releases it.
type groupbaseBlocker struct {
	*cascadeFakeProxy
	started chan struct{}
	gate    chan struct{}
}

func (b *groupbaseBlocker) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	select {
	case b.started <- struct{}{}:
	default:
	}
	<-b.gate
	return 10, nil
}

func newLaneGroup(t *testing.T, name string, members ...C.Proxy) *GroupBase {
	t.Helper()

	base, err := NewGroupBase(GroupBaseOption{
		Name:      name,
		Type:      C.URLTest,
		Providers: []P.ProxyProvider{&fakeProvider{name: name + "-provider", proxies: members}},
	})
	require.NoError(t, err)
	return base
}

// A group with nothing measured must take the free slots before a group whose
// sweep is only a background refresh: the shared queue is where the two checks
// compete, and the cold group is the one a dial may be waiting on.
func TestGroupBaseURLTestColdGroupOvertakesWarmSweep(t *testing.T) {
	original := probequeue.GetConcurrency()
	probequeue.SetConcurrency(1)
	t.Cleanup(func() { probequeue.SetConcurrency(original) })

	const url = "https://example.com/204"
	started := make(chan struct{}, 1)
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)

	blocker := &groupbaseBlocker{
		cascadeFakeProxy: newCascadeTestProxy("blocker"),
		started:          started,
		gate:             gate,
	}
	block := probequeue.Submit("block", []probequeue.Probe{
		{Proxy: blocker, URL: url, Timeout: 5 * time.Second},
	}, nil)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the blocker must hold the only slot")
	}

	order := &dialLog{}
	warm := &groupbaseLaneProxy{poolOrderedProxy: newPoolOrderedProxy("warm", true, 120), log: order}
	cold := &groupbaseLaneProxy{poolOrderedProxy: newPoolOrderedProxy("cold", false, 0), log: order}
	warmGroup := newLaneGroup(t, "warm-group", warm)
	coldGroup := newLaneGroup(t, "cold-group", cold)

	warmDone := make(chan struct{})
	go func() {
		defer close(warmDone)
		_, _ = warmGroup.URLTest(context.Background(), url, utils.IntRanges[uint16]{})
	}()
	// The warm check submits while the slot is taken, so it waits in its lane.
	time.Sleep(150 * time.Millisecond)

	coldDone := make(chan struct{})
	go func() {
		defer close(coldDone)
		_, _ = coldGroup.URLTest(context.Background(), url, utils.IntRanges[uint16]{})
	}()
	time.Sleep(150 * time.Millisecond)

	release()
	require.True(t, block.Wait(context.Background()))

	for name, done := range map[string]chan struct{}{"warm": warmDone, "cold": coldDone} {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("the %s check must finish", name)
		}
	}

	require.Equal(t, []string{"cold", "warm"}, order.list(),
		"a cold group's check must take the free slot before a warm group's sweep")
}
