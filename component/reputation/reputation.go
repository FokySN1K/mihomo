// Package reputation keeps a per-node score of how well a proxy carried real
// traffic. The score rises slowly with every successful dial and falls fast
// with every failure, so a node that worked for days keeps its place after one
// outage while a node that never answers sinks out of the way. An untouched
// score fades with a half-life, so a node that was dead on another network is
// not avoided forever. With `profile: store-reputation` enabled the score
// survives restarts through cache.db; otherwise it lives in memory only.
package reputation

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/profile"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/log"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	// HalfLife is how long an untouched score needs to fall back to half of
	// its value. Six hours is one mobile session: a node that was dead on the
	// previous network starts the next one without a verdict against it.
	HalfLife = 6 * time.Hour
	// Trusted is the score at which a node is preferred over an unknown one.
	Trusted = 30
	// Suspect is the score at which a node is treated as likely dead.
	Suspect = -30

	scoreMax = 100
	scoreMin = -100

	// dialRiseStep and dialRiseStreak make the rise slow: every success adds
	// one point, and only a streak of five successes widens the step.
	dialRiseStep   = 1
	dialRiseStreak = 5
	dialRiseMax    = 5
	// dialFall is the base drop of a failed dial; every consecutive failure
	// doubles it, so a node that fails twice is out of the neutral band.
	dialFall = 25
	// probeFall is the drop of a failed health probe. It is weaker evidence
	// than a failed dial: the probe carries no application traffic.
	probeFall = 10
	// recoveryStreak is how many successful dials in a row lift a suspect
	// node back to neutral: the network changed and it works again.
	recoveryStreak = 3

	maxEntries = 4096
	// flushDelay groups the writes of a busy dial phase into one transaction.
	flushDelay = 15 * time.Second
)

type entry struct {
	Score    int16     `msgpack:"s"`
	Streak   int16     `msgpack:"k"`
	Fails    int16     `msgpack:"f"`
	LastSeen time.Time `msgpack:"t"`
}

// decayed applies the half-life to a score that was not refreshed for a while.
func (e *entry) decayed(now time.Time) int {
	if e.Score == 0 {
		return 0
	}
	age := now.Sub(e.LastSeen)
	if age <= 0 {
		return int(e.Score)
	}
	score := float64(e.Score) * math.Pow(0.5, float64(age)/float64(HalfLife))
	return clamp(int(math.Round(score)))
}

func clamp(score int) int {
	if score > scoreMax {
		return scoreMax
	}
	if score < scoreMin {
		return scoreMin
	}
	return score
}

// backend is where a flushed store lands. Tests substitute their own, the
// process uses cache.db.
type backend interface {
	Load() map[string][]byte
	Save(entries map[string][]byte, deletes []string)
}

type cacheBackend struct{}

func (cacheBackend) Load() map[string][]byte {
	return cachefile.Cache().ReputationMap()
}

func (cacheBackend) Save(entries map[string][]byte, deletes []string) {
	cachefile.Cache().SetReputations(entries, deletes)
}

// Store keeps the reputation of every node the process tried to dial. One
// store is shared by all groups: a node that carried traffic in one group is
// known to the others as well.
type Store struct {
	mu      sync.RWMutex
	backend backend
	loaded  bool
	entries map[string]*entry
	dirty   map[string]struct{}
	// deletes holds the names that left the store since the last flush, so
	// the rows of an evicted node do not stay in the cache forever.
	deletes map[string]struct{}
	timer   *time.Timer
}

var defaultStore = newStore(cacheBackend{})

// Default returns the store shared by every group of the process.
func Default() *Store {
	return defaultStore
}

// NewStore returns an isolated in-memory store. A group uses Default(); an
// isolated store keeps its own history, which is what tests want.
func NewStore() *Store {
	return newStore(nil)
}

func newStore(backend backend) *Store {
	return &Store{
		backend: backend,
		entries: make(map[string]*entry),
		dirty:   make(map[string]struct{}),
		deletes: make(map[string]struct{}),
	}
}

// Score returns the current decayed score of the node.
func (s *Store) Score(name string) int {
	if s == nil {
		return 0
	}

	// The dial path asks for the score of every candidate at once, so the
	// steady state reads under the shared lock; only the first call that has
	// to restore the stored scores takes the exclusive one.
	s.mu.RLock()
	if s.loaded || s.backend == nil || !profile.StoreReputation.Load() {
		score := 0
		if e := s.entries[name]; e != nil {
			score = e.decayed(time.Now())
		}
		s.mu.RUnlock()
		return score
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.loadLocked()
	if e := s.entries[name]; e != nil {
		return e.decayed(time.Now())
	}
	return 0
}

// DialSucceeded records a dial that carried real traffic.
func (s *Store) DialSucceeded(name string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.loadLocked()
	now := time.Now()
	e := s.entryLocked(name)
	base := e.decayed(now)
	if base <= Suspect && e.Streak+1 >= recoveryStreak {
		// the node failed before, and the network has changed: it answers
		// again, so it leaves the suspect band at once
		e.Score = 0
	} else {
		step := dialRiseStep + int(e.Streak)/dialRiseStreak
		if step > dialRiseMax {
			step = dialRiseMax
		}
		e.Score = int16(clamp(base + step))
	}
	if e.Streak < 1000 {
		e.Streak++
	}
	e.Fails = 0
	e.LastSeen = now
	s.markDirtyLocked(name)
}

// DialFailed records a dial the node did not answer.
func (s *Store) DialFailed(name string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.loadLocked()
	now := time.Now()
	e := s.entryLocked(name)
	fails := e.Fails
	if fails > 3 {
		fails = 3
	}
	e.Score = int16(clamp(e.decayed(now) - (dialFall << fails)))
	if e.Fails < 30 {
		e.Fails++
	}
	e.Streak = 0
	e.LastSeen = now
	s.markDirtyLocked(name)
}

// ProbeFailed records a health probe the node did not answer. A probe is
// weaker evidence than a dial: it lowers the score but neither escalates the
// failure streak nor resets a success streak.
func (s *Store) ProbeFailed(name string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.loadLocked()
	now := time.Now()
	e := s.entryLocked(name)
	e.Score = int16(clamp(e.decayed(now) - probeFall))
	e.LastSeen = now
	s.markDirtyLocked(name)
}

// Flush writes what changed since the last flush in one transaction and bounds
// the store at the same time. The store calls it on its own after a short
// delay; tests call it directly. A store without persistence still runs the
// trim, so a profile that keeps no scores does not grow the map past the cap.
func (s *Store) Flush() {
	if s == nil {
		return
	}

	s.mu.Lock()
	s.timer = nil
	s.loadLocked()
	s.addDeletesLocked(s.evictLocked())

	if s.backend == nil || !profile.StoreReputation.Load() {
		// nothing of this store may reach cache.db
		s.dirty = make(map[string]struct{})
		s.deletes = make(map[string]struct{})
		s.mu.Unlock()
		return
	}
	if len(s.dirty) == 0 && len(s.deletes) == 0 {
		s.mu.Unlock()
		return
	}

	type snapshot struct {
		name  string
		entry entry
	}
	pending := make([]snapshot, 0, len(s.dirty))
	for name := range s.dirty {
		if e := s.entries[name]; e != nil {
			pending = append(pending, snapshot{name: name, entry: *e})
		}
	}
	deletes := make([]string, 0, len(s.deletes))
	for name := range s.deletes {
		deletes = append(deletes, name)
	}
	s.dirty = make(map[string]struct{})
	s.deletes = make(map[string]struct{})
	s.mu.Unlock()

	// The encoding runs outside the lock: a dial must not wait for a
	// transaction of thousands of entries to finish.
	entries := make(map[string][]byte, len(pending))
	for _, item := range pending {
		data, err := msgpack.Marshal(item.entry)
		if err != nil {
			continue
		}
		entries[item.name] = data
	}

	s.backend.Save(entries, deletes)
}

func (s *Store) entryLocked(name string) *entry {
	e := s.entries[name]
	if e == nil {
		e = &entry{}
		s.entries[name] = e
	}
	return e
}

func (s *Store) markDirtyLocked(name string) {
	// Only a persisting store records what it has to write; the bound of the
	// map must hold either way, so a store that grows past the cap arms the
	// timer even when there is nothing to save.
	if s.backend != nil && profile.StoreReputation.Load() {
		s.dirty[name] = struct{}{}
	}
	if s.timer == nil && (len(s.dirty) > 0 || len(s.entries) > maxEntries) {
		s.timer = time.AfterFunc(flushDelay, s.Flush)
	}
}

// addDeletesLocked records names that left the store so the next flush deletes
// their rows from the cache as well.
func (s *Store) addDeletesLocked(names []string) {
	if len(names) == 0 {
		return
	}
	for _, name := range names {
		delete(s.dirty, name)
		s.deletes[name] = struct{}{}
	}
	if s.backend != nil && s.timer == nil {
		s.timer = time.AfterFunc(flushDelay, s.Flush)
	}
}

// loadLocked restores the persisted scores once, and only while persistence is
// enabled: a store that was never asked to keep its scores must not open the
// cache file. A score measured in this process is newer than the stored one
// and keeps its place, and the rows that do not fit the cap leave the store
// and the cache together instead of being dropped at random.
func (s *Store) loadLocked() {
	if s.loaded || s.backend == nil || !profile.StoreReputation.Load() {
		return
	}
	s.loaded = true

	restored := 0
	for name, data := range s.backend.Load() {
		if _, ok := s.entries[name]; ok {
			continue
		}
		var e entry
		if err := msgpack.Unmarshal(data, &e); err != nil {
			continue
		}
		s.entries[name] = &e
		restored++
	}
	s.addDeletesLocked(s.evictLocked())
	if restored > 0 {
		log.Debugln("[Reputation] restored %d nodes from cache", restored)
	}
}

// evictLocked bounds the store: the nodes with the lowest score and the
// oldest evidence leave first, so a subscription that rotates servers does not
// grow the file forever.
func (s *Store) evictLocked() []string {
	if len(s.entries) <= maxEntries {
		return nil
	}

	type item struct {
		name  string
		score int
		seen  time.Time
	}
	items := make([]item, 0, len(s.entries))
	now := time.Now()
	for name, e := range s.entries {
		items = append(items, item{name: name, score: e.decayed(now), seen: e.LastSeen})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].score != items[j].score {
			return items[i].score < items[j].score
		}
		if !items[i].seen.Equal(items[j].seen) {
			return items[i].seen.Before(items[j].seen)
		}
		return items[i].name < items[j].name
	})

	deletes := make([]string, 0, len(items)-maxEntries)
	for _, it := range items[:len(items)-maxEntries] {
		delete(s.entries, it.name)
		deletes = append(deletes, it.name)
	}
	return deletes
}
