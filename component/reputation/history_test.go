package reputation

import (
	"testing"
)

func TestHistoryRecordsScoreChanges(t *testing.T) {
	store := NewStore()

	store.DialFailed("node")
	events := store.History("node", 0)
	if len(events) != 1 {
		t.Fatalf("expected one event, got %d", len(events))
	}
	first := events[0]
	if first.From != 0 || first.To != -dialFall || first.Delta != -dialFall || first.Reason != EventDialFailed {
		t.Fatalf("unexpected first event: %+v", first)
	}

	store.ProbeFailed("node")
	events = store.History("node", 0)
	if len(events) != 2 {
		t.Fatalf("expected two events, got %d", len(events))
	}
	last := events[len(events)-1]
	if last.From != -dialFall || last.To != -dialFall-probeFall || last.Reason != EventProbeFailed {
		t.Fatalf("unexpected second event: %+v", last)
	}
}

func TestHistoryKeepsNewestWindow(t *testing.T) {
	store := NewStore()
	// A rise fills the window with one event per point, and the failures after
	// it add four more, so the store has more events than the window holds.
	for i := 0; i < scoreMax*2; i++ {
		store.DialSucceeded("node")
	}
	for i := 0; i < 4; i++ {
		store.DialFailed("node")
	}

	events := store.History("node", 0)
	if len(events) != HistoryLimit {
		t.Fatalf("expected a bounded window of %d, got %d", HistoryLimit, len(events))
	}
	for i := 1; i < len(events); i++ {
		if events[i].Time.Before(events[i-1].Time) {
			t.Fatalf("events are not in chronological order at %d", i)
		}
	}

	limited := store.History("node", 3)
	if len(limited) != 3 {
		t.Fatalf("expected three events, got %d", len(limited))
	}
	if limited[len(limited)-1] != events[len(events)-1] {
		t.Fatal("the limited answer must end with the newest event")
	}
}

func TestHistorySkipsUnchangedScores(t *testing.T) {
	store := NewStore()
	for i := 0; i < scoreMax*2; i++ {
		store.DialSucceeded("node")
	}
	events := store.History("node", 0)
	if len(events) == 0 {
		t.Fatal("a rising score left no events")
	}
	if events[len(events)-1].To != scoreMax {
		t.Fatalf("last event ended at %d, want %d", events[len(events)-1].To, scoreMax)
	}

	// A score that sits at the cap no longer moves; the store must not fill
	// the window with changes that did not happen.
	before := len(events)
	for i := 0; i < 10; i++ {
		store.DialSucceeded("node")
	}
	if after := len(store.History("node", 0)); after != before {
		t.Fatalf("unchanged scores were recorded: %d -> %d events", before, after)
	}
}
