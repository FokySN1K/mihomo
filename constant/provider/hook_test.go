package provider

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnLoadedDeliversUntilCancelled(t *testing.T) {
	var (
		mu  sync.Mutex
		got []string
	)
	cancel := OnLoaded(func(name string) {
		mu.Lock()
		got = append(got, name)
		mu.Unlock()
	})

	NotifyLoaded("a")
	cancel()
	NotifyLoaded("b")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"a"}, got)
}
