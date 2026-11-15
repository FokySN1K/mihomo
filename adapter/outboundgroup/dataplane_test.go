package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/common/buf"
	"github.com/metacubex/mihomo/common/callback"
	"github.com/metacubex/mihomo/component/iface"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

// dataPlaneHandshake is the start of a TLS record: the first write the observer
// is allowed to treat as a handshake that must be answered soon.
var dataPlaneHandshake = []byte{0x16, 0x03, 0x01, 0x00, 0x2e, 0x01, 0x00, 0x00, 0x2a}

// dataPlaneTestConn is a connection a test drives by hand: it records the
// writes, answers the reads from a channel, and reports whether its protocol
// needs a handshake, which is what the groups use to decide where the dial is
// credited.
type dataPlaneTestConn struct {
	*cascadeFakeConn
	handshake bool

	reads  chan dataPlaneTestRead
	closed chan struct{}
	once   sync.Once

	mu     sync.Mutex
	writes [][]byte
}

type dataPlaneTestRead struct {
	data []byte
	err  error
}

func newDataPlaneTestConn(handshake bool) *dataPlaneTestConn {
	return &dataPlaneTestConn{
		cascadeFakeConn: &cascadeFakeConn{},
		handshake:       handshake,
		reads:           make(chan dataPlaneTestRead, 8),
		closed:          make(chan struct{}),
	}
}

// NeedHandshake implements N.EarlyConn: the dial of this protocol only accepts
// the connection, the first answered byte is what proves the node.
func (c *dataPlaneTestConn) NeedHandshake() bool { return c.handshake }

func (c *dataPlaneTestConn) answer(data []byte) { c.reads <- dataPlaneTestRead{data: data} }
func (c *dataPlaneTestConn) fail(err error)     { c.reads <- dataPlaneTestRead{err: err} }

func (c *dataPlaneTestConn) Read(b []byte) (int, error) {
	select {
	case r := <-c.reads:
		if r.err != nil {
			return 0, r.err
		}
		return copy(b, r.data), nil
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *dataPlaneTestConn) ReadBuffer(buffer *buf.Buffer) error {
	data := make([]byte, buffer.FreeLen())
	n, err := c.Read(data)
	if n > 0 {
		_, _ = buffer.Write(data[:n])
	}
	return err
}

func (c *dataPlaneTestConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), b...))
	c.mu.Unlock()
	return len(b), nil
}

func (c *dataPlaneTestConn) WriteBuffer(buffer *buf.Buffer) error {
	_, err := c.Write(buffer.Bytes())
	return err
}

func (c *dataPlaneTestConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *dataPlaneTestConn) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

// A dial of a handshake protocol proves the route, not the carrier: the pool
// must install the observer and credit the member only when it answers.
func TestURLTestPoolObservesDataPlaneAndCreditsFirstByte(t *testing.T) {
	inner := newDataPlaneTestConn(true)
	proxy := newPoolControlledProxy("a", func(context.Context, *C.Metadata) (C.Conn, error) {
		return inner, nil
	}, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{AttemptTimeout: intPtr(1500)}, proxy)

	conn, err := pool.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.True(t, callback.IsDataPlaneObserved(conn), "the pool must watch the data plane")

	require.Equal(t, 0, pool.reputation.Score("a"), "an accepted dial is not proof the node carries traffic")
	pool.mu.RLock()
	require.Equal(t, "a", pool.lastGood, "the route itself is already known to work")
	pool.mu.RUnlock()

	_, err = conn.Write(dataPlaneHandshake)
	require.NoError(t, err)
	inner.answer([]byte{0x02})
	_, err = conn.Read(make([]byte, 16))
	require.NoError(t, err)

	require.Greater(t, pool.reputation.Score("a"), 0, "the first answered byte proves the node")

	pool.mu.RLock()
	_, marked := pool.dataMarks["a"]
	sample := pool.firstBytes["a"]
	pool.mu.RUnlock()
	require.False(t, marked)
	require.NotNil(t, sample, "the answer time sizes the watchdog of the next connection")
	require.Equal(t, 1, sample.hits)
}

// A member that accepted the handshake and went silent leaves the next choices
// and is reported to the API, without a probe having to discover it.
func TestURLTestPoolSilentMemberLeavesChoices(t *testing.T) {
	a := newPoolControlledProxy("a", nil, poolProbeOk(10))
	b := newPoolControlledProxy("b", nil, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{}, a, b)
	require.Equal(t, 2, pool.UsableMembers())

	pool.onDataSilent(a, errors.New("connection reset by peer"))

	require.Equal(t, 1, pool.UsableMembers(), "a silent member cannot carry the next flow")

	pool.mu.RLock()
	mark := pool.dataMarks["a"]
	pool.mu.RUnlock()
	require.NotNil(t, mark)
	require.Equal(t, 1, mark.strikes)
	require.WithinDuration(t, time.Now().Add(urlTestPoolDataStrike), mark.until, 2*time.Second)

	data, err := pool.MarshalJSON()
	require.NoError(t, err)
	var state struct {
		DataSilent []string `json:"dataSilent"`
	}
	require.NoError(t, json.Unmarshal(data, &state))
	require.Equal(t, []string{"a"}, state.DataSilent)
}

// A member that keeps dying after the handshake backs off further every strike,
// up to the cap, so it cannot cost a broken flow every reconnect.
func TestURLTestPoolSilentStrikesDoubleTheTTL(t *testing.T) {
	a := newPoolControlledProxy("a", nil, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{}, a)

	ttls := []time.Duration{
		urlTestPoolDataStrike,
		2 * urlTestPoolDataStrike,
		4 * urlTestPoolDataStrike,
		8 * urlTestPoolDataStrike,
		urlTestPoolDataStrikeMax,
		urlTestPoolDataStrikeMax,
	}
	for i, ttl := range ttls {
		pool.onDataSilent(a, errors.New("connection reset by peer"))

		pool.mu.RLock()
		mark := pool.dataMarks["a"]
		pool.mu.RUnlock()
		require.NotNil(t, mark)
		require.Equal(t, i+1, mark.strikes)
		require.InDelta(t, float64(ttl), float64(time.Until(mark.until)), float64(time.Second),
			"strike %d must back off for %s", i+1, ttl)
	}

	require.Less(t, pool.reputation.Score("a"), 0, "a member that died twice after the handshake is suspect")
}

// A probe reaches the test URL; the flow that died was something else. The
// probe must not clear the evidence the data plane left.
func TestURLTestPoolProbeDoesNotClearSilentMark(t *testing.T) {
	a := newPoolControlledProxy("a", nil, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{}, a)

	pool.onDataSilent(a, errors.New("connection reset by peer"))
	pool.quickCheck()

	pool.mu.RLock()
	mark := pool.dataMarks["a"]
	pool.mu.RUnlock()
	require.NotNil(t, mark, "a probe answers one URL, not the flow that went silent")
	require.Equal(t, 0, pool.UsableMembers(), "the member stays out of the parent's count as well")
}

// A real answer is the proof: it clears the mark at once, without waiting for
// the demotion to expire.
func TestURLTestPoolAnswerClearsSilentMark(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newPoolControlledProxy("a", nil, poolProbeOk(10)))

	proxy := pool.GetProxies(false)[0]
	pool.onDataSilent(proxy, errors.New("connection reset by peer"))
	pool.noteDataAnswered(proxy, 20*time.Millisecond)

	pool.mu.RLock()
	_, marked := pool.dataMarks["a"]
	sample := pool.firstBytes["a"]
	pool.mu.RUnlock()
	require.False(t, marked)
	require.NotNil(t, sample)
	require.Equal(t, 20*time.Millisecond, sample.avg)
}

// The watchdog follows the answer time of the member: the floor keeps a fast
// node from being cut on jitter, a slow one gets a wider budget, and the cap
// keeps a pathological one from hiding a dead node for long.
func TestURLTestPoolFirstByteWatchdogBounds(t *testing.T) {
	proxy := newPoolControlledProxy("a", nil, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{}, proxy)

	require.Equal(t, urlTestPoolFirstByteFloor, pool.firstByteWatchdog("a"), "an unknown member uses the floor")

	pool.noteDataAnswered(proxy, 100*time.Millisecond)
	require.Equal(t, urlTestPoolFirstByteFloor, pool.firstByteWatchdog("a"), "a fast member stays on the floor")

	pool.noteDataAnswered(proxy, time.Second)
	require.InDelta(t, float64(975*time.Millisecond), float64(pool.firstByteWatchdog("a")), float64(10*time.Millisecond))

	pool.noteDataAnswered(proxy, 10*time.Second)
	require.Equal(t, urlTestPoolFirstByteCap, pool.firstByteWatchdog("a"), "a very slow member stays under the cap")
}

// A connection that is already observed must not be wrapped again: one dead
// flow must cost one demotion, not one per wrapping level.
func TestURLTestPoolDoesNotWrapObservedConnTwice(t *testing.T) {
	pool := newPoolForTest(t, URLTestPoolOption{}, newPoolControlledProxy("a", nil, poolProbeOk(10)))
	proxy := pool.GetProxies(false)[0]

	wrapped := callback.NewDataPlaneConn(newDataPlaneTestConn(true), callback.DataPlaneOptions{})
	require.Same(t, wrapped, pool.observeDataPlane(proxy, wrapped, false))
}

// The cascade credits the member at its first answered byte for a handshake
// protocol, and the observer is installed exactly once.
func TestCascadeObservesDataPlaneOnDial(t *testing.T) {
	inner := newDataPlaneTestConn(true)
	proxy := newCascadeFakeProxy("a", C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
		return inner, nil
	}, nil, 100)
	group := newCascadeForTest(t, CascadeOption{}, proxy)

	conn, err := group.DialContext(context.Background(), &C.Metadata{})
	require.NoError(t, err)
	require.True(t, callback.IsDataPlaneObserved(conn), "the cascade must watch the data plane")

	group.mu.RLock()
	require.Equal(t, "a", group.active)
	require.Empty(t, group.states["a"].dataUntil, "an accepted dial says nothing yet")
	group.mu.RUnlock()

	_, err = conn.Write(dataPlaneHandshake)
	require.NoError(t, err)
	inner.answer([]byte{0x02})
	_, err = conn.Read(make([]byte, 16))
	require.NoError(t, err)

	group.mu.RLock()
	state := group.states["a"]
	group.mu.RUnlock()
	require.NotNil(t, state)
	require.Zero(t, state.dataCount)
}

// A candidate that accepted the handshake and went silent is demoted at once,
// loses the active slot, and is reported without the failed-dial cooldown of a
// member that never connected at all.
func TestCascadeSilentCandidateIsDemotedAtOnce(t *testing.T) {
	a := newCascadeTestProxy("a")
	b := newCascadeTestProxy("b")
	group := newCascadeForTest(t, CascadeOption{}, a, b)

	group.active = "a"
	group.onDataSilent(a, errors.New("connection reset by peer"))

	group.mu.RLock()
	state := group.states["a"]
	group.mu.RUnlock()
	require.NotNil(t, state)
	require.WithinDuration(t, time.Now().Add(cascadeDataCooldown), state.dataUntil, 2*time.Second)
	require.Empty(t, group.active, "the silent candidate cannot stay active")
	require.Equal(t, 1, group.UsableMembers())

	order := group.candidateOrder(group.GetProxies(false))
	require.Equal(t, "b", order[0].Name())
	require.Equal(t, "a", order[len(order)-1].Name(), "a cooling candidate goes to the end of the order")

	group.onDataSilent(a, errors.New("connection reset by peer"))
	group.mu.RLock()
	state = group.states["a"]
	group.mu.RUnlock()
	require.Equal(t, 2, state.dataCount)
	require.WithinDuration(t, time.Now().Add(2*cascadeDataCooldown), state.dataUntil, 2*time.Second,
		"a candidate that repeats the failure backs off further")
}

// A probe cannot prove what a dead flow denied: a candidate measured alive must
// keep its data-plane demotion until real traffic answers.
func TestCascadeProbeDoesNotClearSilentMark(t *testing.T) {
	a := newCascadeTestProxy("a")
	group := newCascadeForTest(t, CascadeOption{}, a)

	group.onDataSilent(a, errors.New("connection reset by peer"))
	group.markAlive("a", 100)

	group.mu.RLock()
	state := group.states["a"]
	group.mu.RUnlock()
	require.NotNil(t, state)
	require.True(t, state.alive, "the probe did answer")
	require.False(t, state.dataUntil.IsZero(), "but it says nothing about the flow that died")
	require.Equal(t, 0, group.UsableMembers())

	group.onDataAnswered("a")
	group.mu.RLock()
	state = group.states["a"]
	group.mu.RUnlock()
	require.True(t, state.dataUntil.IsZero())
	require.Zero(t, state.dataCount)
	require.Equal(t, 1, group.UsableMembers(), "a real answer returns the candidate at once")
}

// settleActive must not leave a silent candidate active even when the probe
// still marks it alive.
func TestCascadeSettleActiveSkipsSilentCandidate(t *testing.T) {
	a := newCascadeTestProxy("a")
	b := newCascadeTestProxy("b")
	group := newCascadeForTest(t, CascadeOption{}, a, b)

	group.mu.Lock()
	group.states["a"] = &cascadeState{alive: true}
	group.states["b"] = &cascadeState{alive: true}
	group.active = "a"
	group.mu.Unlock()

	group.onDataSilent(a, errors.New("connection reset by peer"))
	group.settleActive(group.GetProxies(false))

	group.mu.RLock()
	active := group.active
	group.mu.RUnlock()
	require.Equal(t, "b", active)
}

// An observed connection is never wrapped twice by the cascade either.
func TestCascadeDoesNotWrapObservedConnTwice(t *testing.T) {
	proxy := newCascadeTestProxy("a")
	group := newCascadeForTest(t, CascadeOption{}, proxy)

	wrapped := callback.NewDataPlaneConn(newDataPlaneTestConn(true), callback.DataPlaneOptions{})
	require.Same(t, wrapped, group.observeDataPlane(proxy, wrapped))
}

// newDataPlaneGroupProviders wraps the members of a group a test builds.
func newDataPlaneGroupProviders(t *testing.T, name string, proxies []C.Proxy) []P.ProxyProvider {
	t.Helper()

	pd, err := provider.NewCompatibleProvider(name, proxies,
		provider.NewHealthCheck(nil, "", 0, 0, false, nil))
	require.NoError(t, err)
	return []P.ProxyProvider{pd}
}

// dataPlaneGroupBuilders lists the groups that do not rank their members
// themselves but still have to watch the data plane.
func dataPlaneGroupBuilders() []struct {
	name  string
	build func(t *testing.T, proxies []C.Proxy) C.ProxyAdapter
} {
	return []struct {
		name  string
		build func(t *testing.T, proxies []C.Proxy) C.ProxyAdapter
	}{
		{"urltest", func(t *testing.T, proxies []C.Proxy) C.ProxyAdapter {
			group, err := NewURLTest(GroupCommonOption{Name: "urltest"}, URLTestOption{},
				newPoolEmptyFallback(), newDataPlaneGroupProviders(t, "urltest", proxies))
			require.NoError(t, err)
			return group
		}},
		{"fallback", func(t *testing.T, proxies []C.Proxy) C.ProxyAdapter {
			group, err := NewFallback(GroupCommonOption{Name: "fallback"}, FallbackOption{},
				newPoolEmptyFallback(), newDataPlaneGroupProviders(t, "fallback", proxies))
			require.NoError(t, err)
			return group
		}},
		{"loadbalance", func(t *testing.T, proxies []C.Proxy) C.ProxyAdapter {
			group, err := NewLoadBalance(GroupCommonOption{Name: "loadbalance"}, LoadBalanceOption{},
				newPoolEmptyFallback(), newDataPlaneGroupProviders(t, "loadbalance", proxies))
			require.NoError(t, err)
			return group
		}},
	}
}

// The groups that do not rank their members still must not credit a node that
// only accepted the handshake: they install the same observer, and a silent
// flow is closed and reported as a failed dial their health check reacts to.
func TestSimpleGroupsObserveHandshakeDataPlane(t *testing.T) {
	for _, build := range dataPlaneGroupBuilders() {
		for _, handshake := range []bool{true, false} {
			label := "plain"
			if handshake {
				label = "handshake"
			}
			t.Run(build.name+"/"+label, func(t *testing.T) {
				inner := newDataPlaneTestConn(handshake)
				proxy := newCascadeFakeProxy("a", C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
					return inner, nil
				}, nil, 100)

				conn, err := build.build(t, []C.Proxy{proxy}).DialContext(context.Background(), &C.Metadata{})
				require.NoError(t, err)
				require.Equal(t, handshake, callback.IsDataPlaneObserved(conn),
					"only a protocol whose dial says nothing about the carrier needs the observer")
				require.NoError(t, conn.Close())
			})
		}
	}
}

// A nested group may already watch its connection: the outer group must pass
// it through, or one dead flow would cost one demotion per wrapping level.
func TestSimpleGroupsDoNotWrapObservedConnTwice(t *testing.T) {
	for _, build := range dataPlaneGroupBuilders() {
		t.Run(build.name, func(t *testing.T) {
			observed := callback.NewDataPlaneConn(newDataPlaneTestConn(true), callback.DataPlaneOptions{})
			proxy := newCascadeFakeProxy("a", C.Shadowsocks, func(context.Context, *C.Metadata) (C.Conn, error) {
				return observed, nil
			}, nil, 100)

			conn, err := build.build(t, []C.Proxy{proxy}).DialContext(context.Background(), &C.Metadata{})
			require.NoError(t, err)
			require.True(t, conn == observed, "an already observed connection must be passed through")
			require.NoError(t, conn.Close())
		})
	}
}

// A new network invalidates the silence measured on the old one: the same node
// often works after the device moves.
func TestURLTestPoolNetworkChangeClearsDataMarks(t *testing.T) {
	a := newPoolControlledProxy("a", nil, poolProbeOk(10))
	pool := newPoolForTest(t, URLTestPoolOption{}, a)
	pool.onDataSilent(a, callback.ErrHandshakeUnanswered)

	pool.mu.RLock()
	_, marked := pool.dataMarks["a"]
	pool.mu.RUnlock()
	require.True(t, marked)

	iface.NotifyNetworkChanged()
	require.True(t, pool.dropBackoffOnNetworkChange())

	pool.mu.RLock()
	_, marked = pool.dataMarks["a"]
	pool.mu.RUnlock()
	require.False(t, marked, "the new route has to prove itself from scratch")
}
