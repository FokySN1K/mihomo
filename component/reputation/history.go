package reputation

import "time"

// HistoryLimit is how many score changes one node keeps. A node changes its
// score on every connection until it reaches the band it belongs to, so the
// window is a bound of the debug view and not a promise that every change of a
// long session is still readable.
const HistoryLimit = 32

// Event is one change of a node's score, kept for debugging. The history is
// in memory only: it explains a score to its reader, it is not a part of the
// persisted reputation and never reaches cache.db.
type Event struct {
	Time   time.Time `json:"time"`
	Node   string    `json:"node"`
	From   int       `json:"from"`
	To     int       `json:"to"`
	Delta  int       `json:"delta"`
	Reason string    `json:"reason"`
}

// Reasons of a score change. They are part of the REST contract; a host
// renders an unknown reason as is.
const (
	EventDialSucceeded = "dial-ok"
	EventDialFailed    = "dial-fail"
	EventProbeFailed   = "probe-fail"
	EventDataFailed    = "data-fail"
)

// History returns the newest score changes of the node in chronological
// order. A limit above HistoryLimit is capped; a non-positive limit asks for
// the whole per-node window.
func (s *Store) History(name string, limit int) []Event {
	if s == nil {
		return nil
	}
	if limit <= 0 || limit > HistoryLimit {
		limit = HistoryLimit
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	events := s.history[name]
	if len(events) > limit {
		events = events[len(events)-limit:]
	}
	out := make([]Event, len(events))
	copy(out, events)
	return out
}

// recordChangeLocked files one score change. It runs under the store lock,
// next to the mutation that produced the score, so the history cannot show a
// change the store did not make. It must be called with s.mu held.
func (s *Store) recordChangeLocked(name string, from, to int, reason string) {
	if name == "" || from == to {
		return
	}
	if s.history == nil {
		s.history = make(map[string][]Event)
	}
	events := append(s.history[name], Event{
		Time:   time.Now(),
		Node:   name,
		From:   from,
		To:     to,
		Delta:  to - from,
		Reason: reason,
	})
	if len(events) > HistoryLimit {
		events = events[len(events)-HistoryLimit:]
	}
	s.history[name] = events
}
