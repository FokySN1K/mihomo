package outboundgroup

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

// The per-group window is what keeps a group with hundreds of nodes from
// turning its history into a log of every node it ever dropped.
func TestGroupHistoryCapsOneGroup(t *testing.T) {
	resetGroupHistory()
	t.Cleanup(resetGroupHistory)

	for i := 0; i < GroupHistoryLimit+8; i++ {
		RecordGroupEvent(GroupEvent{Group: "g", Type: GroupEventNow, To: "n" + strconv.Itoa(i)})
	}

	events := GroupHistory("g", 0)
	require.Len(t, events, GroupHistoryLimit)
	require.Equal(t, "n8", events[0].To, "the oldest events are evicted first")
	require.Equal(t, "n"+strconv.Itoa(GroupHistoryLimit+7), events[len(events)-1].To,
		"the newest event is the last one")
}

// The global cap bounds a profile with hundreds of groups: the store must not
// grow with the subscription size.
func TestGroupHistoryCapsTheWholeStore(t *testing.T) {
	resetGroupHistory()
	t.Cleanup(resetGroupHistory)

	const groups = 100
	for i := 0; i < groups; i++ {
		for j := 0; j < GroupHistoryLimit; j++ {
			RecordGroupEvent(GroupEvent{Group: fmt.Sprintf("g%d", i), Type: GroupEventNow, To: "n"})
		}
	}

	groupHistory.Lock()
	total := len(groupHistory.events)
	groupHistory.Unlock()
	require.LessOrEqual(t, total, TotalHistoryLimit)

	require.NotEmpty(t, GroupHistory("g99", 0), "the newest events survive the compaction")
	require.Empty(t, GroupHistory("g0", 0), "the oldest events are evicted first")
}

// A host asks for the tail of the window: the answer is chronological, and a
// limit above the window is capped instead of widening the store.
func TestGroupHistoryReturnsTheTailInOrder(t *testing.T) {
	resetGroupHistory()
	t.Cleanup(resetGroupHistory)

	for i := 0; i < 5; i++ {
		RecordGroupEvent(GroupEvent{Group: "g", Type: GroupEventNow, To: "n" + strconv.Itoa(i)})
		RecordGroupEvent(GroupEvent{Group: "other", Type: GroupEventNow, To: "x"})
	}

	events := GroupHistory("g", 3)
	require.Len(t, events, 3)
	require.Equal(t, "n2", events[0].To)
	require.Equal(t, "n4", events[2].To)

	require.Len(t, GroupHistory("g", GroupHistoryLimit+100), 5,
		"a limit above the window must not widen it")
}

func TestGroupHistoryNeedsAGroup(t *testing.T) {
	resetGroupHistory()
	t.Cleanup(resetGroupHistory)

	RecordGroupEvent(GroupEvent{Type: GroupEventNow, To: "n"})

	groupHistory.Lock()
	total := len(groupHistory.events)
	groupHistory.Unlock()
	require.Zero(t, total)
}

func TestRecordPinEventNamesTheDirection(t *testing.T) {
	resetGroupHistory()
	t.Cleanup(resetGroupHistory)

	recordPinEvent("g", "a", "b")
	recordPinEvent("g", "b", "")

	events := GroupHistory("g", 0)
	require.Len(t, events, 2)
	require.Equal(t, GroupEventPin, events[0].Type)
	require.Equal(t, "a", events[0].From)
	require.Equal(t, "b", events[0].To)
	require.Equal(t, GroupEventUnpin, events[1].Type)
	require.Equal(t, "b", events[1].From)
}

// A pin is a public statement about the group, so it is recorded; reading the
// same state must not duplicate it, or a host polling the group would fill the
// window with one event.
func TestURLTestPoolRecordsPinAndNowOnce(t *testing.T) {
	a := newPoolOrderedProxy("a", true, 100)
	b := newPoolOrderedProxy("b", true, 200)
	pool := newPoolForTest(t, URLTestPoolOption{}, a, b)
	// a read of the group must not start a check that could change the state
	// the history is being read for
	pool.lazy = true

	resetGroupHistory()
	t.Cleanup(resetGroupHistory)

	pool.ForceSet("b")
	require.Equal(t, "b", pool.Now())
	_, err := pool.MarshalJSON()
	require.NoError(t, err)
	_, err = pool.MarshalJSON()
	require.NoError(t, err)

	var pins, nows int
	var pinnedTo, nowTo string
	for _, event := range GroupHistory("pool", 0) {
		switch event.Type {
		case GroupEventPin:
			pins++
			pinnedTo = event.To
		case GroupEventNow:
			nows++
			nowTo = event.To
		}
	}
	require.Equal(t, 1, pins, "one selection is one event")
	require.Equal(t, "b", pinnedTo)
	require.Equal(t, 1, nows, "reading an unchanged group must not duplicate the event")
	require.Equal(t, "b", nowTo)
}

// A pin holds the route while the member is measured alive. A member the pool
// sees dead is not a route: the pool keeps the pin on record but dials a live
// member and says so, so the card can show the pin is skipped.
func TestURLTestPoolKeepsAPinOnADeadMember(t *testing.T) {
	a := newPoolOrderedProxy("a", true, 100)
	dead := newPoolOrderedProxy("dead", false, 0)
	pool := newPoolForTest(t, URLTestPoolOption{}, a, dead)
	pool.lazy = true

	pool.ForceSet("dead")
	decoded := marshalGroupState(t, pool)
	require.Equal(t, "dead", decoded["fixed"], "the pin stays on record")
	require.Equal(t, false, decoded["fixedActive"], "a dead pin must not claim the route")
	require.Equal(t, NowReasonFixedSuspicious, decoded["nowReason"])
	require.Equal(t, "a", decoded["now"], "traffic must leave a dead pinned member")

	pool.ForceSet("a")
	decoded = marshalGroupState(t, pool)
	require.Equal(t, "a", decoded["fixed"])
	require.Equal(t, true, decoded["fixedActive"])
	require.Equal(t, NowReasonFixed, decoded["nowReason"])
	require.Equal(t, "a", decoded["now"])
}

// A mark a failed dial left on the pinned member takes it out of play while
// the pool has a live member to route through: the pin stays on record, the
// route moves, and the card shows the mark on the pinned member.
func TestURLTestPoolKeepsAPinOnASuspiciousMember(t *testing.T) {
	a := newPoolOrderedProxy("a", true, 100)
	b := newPoolOrderedProxy("b", true, 200)
	pool := newPoolForTest(t, URLTestPoolOption{}, a, b)
	pool.lazy = true

	pool.ForceSet("b")
	pool.markSuspicious("b")

	decoded := marshalGroupState(t, pool)
	require.Equal(t, "b", decoded["fixed"], "the pin stays on record")
	require.Equal(t, false, decoded["fixedActive"], "a marked pin must not claim the route")
	require.Equal(t, NowReasonFixedSuspicious, decoded["nowReason"])
	require.Equal(t, "a", decoded["now"], "traffic must leave a marked pinned member")
}

func marshalGroupState(t *testing.T, group C.ProxyAdapter) map[string]any {
	t.Helper()

	data, err := group.MarshalJSON()
	require.NoError(t, err)

	decoded := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &decoded))
	return decoded
}
