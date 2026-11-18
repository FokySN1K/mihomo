package outboundgroup

import (
	"sync"
	"time"
)

const (
	// GroupHistoryLimit is how many significant events one group keeps. The
	// per-node measurements are not duplicated here: every proxy already
	// carries its own delay history.
	GroupHistoryLimit = 32
	// TotalHistoryLimit bounds the whole store, so a profile with hundreds of
	// groups cannot grow the history with the subscription size.
	TotalHistoryLimit = 2048
)

// GroupEvent is one significant change in the state of a computed group. A
// read of the group state (Now/MarshalJSON) never carries the history; a host
// asks for it on demand.
type GroupEvent struct {
	Time   time.Time `json:"time"`
	Group  string    `json:"group,omitempty"`
	Type   string    `json:"type"`
	From   string    `json:"from,omitempty"`
	To     string    `json:"to,omitempty"`
	Reason string    `json:"reason,omitempty"`
	Detail string    `json:"detail,omitempty"`
}

// Event types. They are part of the REST contract; a host renders an unknown
// type as is.
const (
	// GroupEventNow is a change of the member the group routes through.
	GroupEventNow = "now"
	// GroupEventPin is a manual selection.
	GroupEventPin = "pin"
	// GroupEventUnpin is an explicit removal of a manual selection.
	GroupEventUnpin = "unpin"
	// GroupEventSuspicious is a member that a failed dial or a silent data
	// plane marked out of play.
	GroupEventSuspicious = "suspicious"
	// GroupEventRecovered is a member that answered again.
	GroupEventRecovered = "recovered"
	// GroupEventSilent is a member that accepted the handshake and went
	// silent.
	GroupEventSilent = "data-silent"
	// GroupEventPool is a change of the pool composition or size.
	GroupEventPool = "pool"
)

type groupHistoryStore struct {
	sync.Mutex
	events []GroupEvent
	counts map[string]int
}

var groupHistory = groupHistoryStore{counts: make(map[string]int)}

// RecordGroupEvent stores one significant event. The per-group and global
// caps are enforced here, so the store cannot grow without a bound. The oldest
// events are evicted first.
func RecordGroupEvent(event GroupEvent) {
	if event.Group == "" {
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}

	groupHistory.Lock()
	defer groupHistory.Unlock()

	groupHistory.events = append(groupHistory.events, event)
	groupHistory.counts[event.Group]++
	if len(groupHistory.events) <= TotalHistoryLimit && groupHistory.counts[event.Group] <= GroupHistoryLimit {
		return
	}
	compactGroupHistoryLocked()
}

// compactGroupHistoryLocked keeps the newest events, at most
// GroupHistoryLimit per group and TotalHistoryLimit overall.
func compactGroupHistoryLocked() {
	events := groupHistory.events
	kept := make([]GroupEvent, 0, len(events))
	counts := make(map[string]int, len(groupHistory.counts))

	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if len(kept) >= TotalHistoryLimit {
			break
		}
		if counts[event.Group] >= GroupHistoryLimit {
			continue
		}
		counts[event.Group]++
		kept = append(kept, event)
	}

	for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
		kept[i], kept[j] = kept[j], kept[i]
	}
	groupHistory.events = kept
	groupHistory.counts = counts
}

// GroupHistory returns the newest events of one group in chronological order.
// A limit above GroupHistoryLimit is capped; a non-positive limit asks for the
// whole per-group window.
func GroupHistory(group string, limit int) []GroupEvent {
	if limit <= 0 || limit > GroupHistoryLimit {
		limit = GroupHistoryLimit
	}

	groupHistory.Lock()
	defer groupHistory.Unlock()

	events := make([]GroupEvent, 0, limit)
	for i := len(groupHistory.events) - 1; i >= 0 && len(events) < limit; i-- {
		if groupHistory.events[i].Group == group {
			events = append(events, groupHistory.events[i])
		}
	}
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}
	return events
}

// resetGroupHistory drops the store; tests use it to isolate cases.
func resetGroupHistory() {
	groupHistory.Lock()
	groupHistory.events = nil
	groupHistory.counts = make(map[string]int)
	groupHistory.Unlock()
}

// groupName is the name an event is filed under. A group built without a base
// (a bare struct in a unit test) has no name, and the store drops such events.
func groupName(base *GroupBase) string {
	if base == nil {
		return ""
	}
	return base.Name()
}

func recordPinEvent(group, from, to string) {
	eventType := GroupEventPin
	if to == "" {
		eventType = GroupEventUnpin
	}
	RecordGroupEvent(GroupEvent{
		Group: group,
		Type:  eventType,
		From:  from,
		To:    to,
	})
}
