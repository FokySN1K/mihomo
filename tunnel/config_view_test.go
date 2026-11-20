package tunnel

import (
	"sync"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

// An apply replaces the listener map while the REST handlers read it. The
// view must hand out a whole map without racing with the replacement; without
// the atomic view this test fails under -race.
func TestListenersViewIsRaceFree(t *testing.T) {
	original := Listeners()
	t.Cleanup(func() { UpdateListeners(original) })

	replacement := map[string]C.InboundListener{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				UpdateListeners(replacement)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			_ = Listeners()
		}
	}()
	close(stop)
	wg.Wait()
}
