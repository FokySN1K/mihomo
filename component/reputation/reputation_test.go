package reputation

import (
	"fmt"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/profile"

	"github.com/stretchr/testify/require"
)

type fakeBackend struct {
	entries map[string][]byte
	saves   int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{entries: make(map[string][]byte)}
}

func (f *fakeBackend) Load() map[string][]byte {
	return f.entries
}

func (f *fakeBackend) Save(entries map[string][]byte, deletes []string) {
	f.saves++
	for _, name := range deletes {
		delete(f.entries, name)
	}
	for name, data := range entries {
		f.entries[name] = data
	}
}

func enablePersistence(t *testing.T) {
	t.Helper()
	profile.StoreReputation.Store(true)
	t.Cleanup(func() {
		profile.StoreReputation.Store(false)
	})
}

func TestScoreRisesSlowlyAndFallsFast(t *testing.T) {
	store := newStore(nil)

	for i := 0; i < 10; i++ {
		store.DialSucceeded("node")
	}
	require.Equal(t, 15, store.Score("node"), "the step widens with the streak")

	store.DialFailed("node")
	require.Equal(t, -10, store.Score("node"))

	store.DialFailed("node")
	require.Equal(t, -60, store.Score("node"))

	store.DialFailed("node")
	require.Equal(t, -100, store.Score("node"), "the score must not fall below the floor")
}

func TestSuspectNodeRecoversOnAStreak(t *testing.T) {
	store := newStore(nil)

	store.DialFailed("node")
	store.DialFailed("node")
	require.Equal(t, -75, store.Score("node"))
	require.LessOrEqual(t, store.Score("node"), Suspect)

	store.DialSucceeded("node")
	store.DialSucceeded("node")
	require.Equal(t, -73, store.Score("node"), "a single answer must not lift the verdict")

	store.DialSucceeded("node")
	require.Equal(t, 0, store.Score("node"), "three answers in a row lift the suspect band")
}

func TestProbeFailureDoesNotEscalate(t *testing.T) {
	store := newStore(nil)

	store.DialFailed("node")
	store.DialFailed("node")
	require.Equal(t, -75, store.Score("node"))

	store.ProbeFailed("node")
	require.Equal(t, -85, store.Score("node"), "a probe lowers the score by its own step only")

	store.DialSucceeded("node")
	require.Equal(t, -84, store.Score("node"), "a probe must not deepen the dial-failure escalation")
}

func TestScoreDecaysWithHalfLife(t *testing.T) {
	store := newStore(nil)
	store.entries["node"] = &entry{Score: 80, LastSeen: time.Now().Add(-HalfLife)}

	require.Equal(t, 40, store.Score("node"))
}

func TestFlushPersistsAndRestores(t *testing.T) {
	enablePersistence(t)
	backend := newFakeBackend()
	store := newStore(backend)

	for i := 0; i < 4; i++ {
		store.DialSucceeded("node")
	}
	store.Flush()

	require.Equal(t, 1, backend.saves)
	require.Contains(t, backend.entries, "node")

	restored := newStore(backend)
	require.Equal(t, 4, restored.Score("node"), "the score must survive a restart")
}

func TestFlushWithoutPersistenceWritesNothing(t *testing.T) {
	backend := newFakeBackend()
	store := newStore(backend)

	store.DialSucceeded("node")
	store.Flush()

	require.Zero(t, backend.saves)
}

func TestEvictionKeepsTheBestNodes(t *testing.T) {
	store := newStore(nil)
	now := time.Now()
	for i := 0; i < maxEntries+2; i++ {
		store.entries[fmt.Sprintf("node-%d", i)] = &entry{LastSeen: now}
	}
	store.entries["dead"] = &entry{Score: -90, LastSeen: now}
	store.entries["suspect"] = &entry{Score: -40, LastSeen: now}

	deletes := store.evictLocked()

	require.Len(t, deletes, 4)
	require.Contains(t, deletes, "dead")
	require.Contains(t, deletes, "suspect")
	require.Len(t, store.entries, maxEntries)
}
