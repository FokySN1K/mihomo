package outboundgroup

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// A dashboard switch writes the name while dials read it. The race detector
// must see the two sides synchronised; without the atomic pointer this test
// fails under -race.
func TestSelectorSelectedIsRaceFree(t *testing.T) {
	selector := &Selector{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			selector.setSelected("node")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = selector.selectedName()
		}
	}()
	wg.Wait()
	require.Equal(t, "node", selector.selectedName())
}
