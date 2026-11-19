package reputation

import "testing"

func TestSnapshotSortsWorstFirst(t *testing.T) {
	store := NewStore()
	store.DialSucceeded("good")
	store.DialFailed("bad")
	store.DialFailed("bad")
	store.ProbeFailed("weak")

	entries := store.Snapshot()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].Name != "bad" || entries[0].Fails != 2 {
		t.Fatalf("expected the worst node first with two fails, got %+v", entries[0])
	}
	if entries[len(entries)-1].Name != "good" {
		t.Fatalf("expected the best node last, got %+v", entries)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Score > entries[i].Score {
			t.Fatalf("snapshot is not sorted worst first: %+v", entries)
		}
	}
}

func TestSnapshotOfNilStore(t *testing.T) {
	var store *Store
	if entries := store.Snapshot(); entries != nil {
		t.Fatalf("expected no entries from a nil store, got %+v", entries)
	}
}
