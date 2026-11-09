package reputation

import (
	"fmt"
	"testing"
)

// benchmarkNames is the name list a pool of one provider hands to the store:
// prebuilt, so a benchmark measures the store and not the formatting.
func benchmarkNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("node-%03d", i)
	}
	return names
}

// benchStore fills a store with the scores a long session leaves behind.
func benchStore(names []string) *Store {
	store := NewStore()
	for _, name := range names {
		store.DialSucceeded(name)
	}
	return store
}

// BenchmarkStoreScore measures the lookup the pool runs for every candidate of
// a sweep, one after another.
func BenchmarkStoreScore(b *testing.B) {
	names := benchmarkNames(256)
	store := benchStore(names)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.Score(names[i%len(names)])
	}
}

// BenchmarkStoreScoreParallel measures the same lookup from the connections of
// a busy process: every one of them orders the candidates of its own pool.
func BenchmarkStoreScoreParallel(b *testing.B) {
	names := benchmarkNames(256)
	store := benchStore(names)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			store.Score(names[i%len(names)])
		}
	})
}

// BenchmarkStoreDialSucceeded measures what a successful dial records.
func BenchmarkStoreDialSucceeded(b *testing.B) {
	names := benchmarkNames(256)
	store := benchStore(names)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.DialSucceeded(names[i%len(names)])
	}
}

// BenchmarkStoreDialSucceededParallel measures the record of a dial from many
// connections at once, which is the case the store lock has to keep cheap.
func BenchmarkStoreDialSucceededParallel(b *testing.B) {
	names := benchmarkNames(256)
	store := benchStore(names)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			store.DialSucceeded(names[i%len(names)])
		}
	})
}
