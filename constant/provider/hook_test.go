package provider

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnChangeDeliversUntilCancelled(t *testing.T) {
	var (
		mu  sync.Mutex
		got []string
	)
	cancel := OnChange(func(name string) {
		mu.Lock()
		got = append(got, name)
		mu.Unlock()
	})

	NotifyChanged("a")
	cancel()
	NotifyChanged("b")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"a"}, got)
}

func TestNotifyChangedIsolatesPanickingSubscriber(t *testing.T) {
	cancelPanic := OnChange(func(string) { panic("boom") })
	defer cancelPanic()

	var got string
	cancel := OnChange(func(name string) { got = name })
	defer cancel()

	require.NotPanics(t, func() { NotifyChanged("a") })
	require.Equal(t, "a", got, "a panicking subscriber must not hide the change from the others")
}
