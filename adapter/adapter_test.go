package adapter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// A check against a reachable endpoint must report both numbers: the full
// delay the pool orders by and the road time the dial took before the server
// answered.
func TestURLTestRecordsRTT(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	proxy := adapter.NewProxy(outbound.NewDirect())
	delay, err := proxy.URLTest(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatalf("URLTest failed: %v", err)
	}
	if delay == 0 {
		t.Fatal("a completed check reported a zero delay")
	}

	state, ok := proxy.ExtraDelayHistories()[server.URL]
	if !ok || len(state.History) != 1 {
		t.Fatalf("history of the tested URL not recorded: %+v", state.History)
	}
	last := state.History[len(state.History)-1]
	if last.RTT == 0 {
		t.Fatal("a completed dial reported no RTT")
	}
	if last.RTT > last.Delay {
		t.Fatalf("RTT %d exceeds the full delay %d", last.RTT, last.Delay)
	}
}

// A failed check must not invent a road time it never measured.
func TestURLTestFailedCheckHasNoRTT(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close() // nothing listens here any more

	proxy := adapter.NewProxy(outbound.NewDirect())
	if _, err := proxy.URLTest(context.Background(), server.URL, nil); err == nil {
		t.Fatal("a check against a closed server reported success")
	}

	state := proxy.ExtraDelayHistories()[server.URL]
	if len(state.History) != 1 {
		t.Fatalf("failed check not recorded: %+v", state.History)
	}
	if state.History[0].RTT != 0 {
		t.Fatalf("a failed dial reported RTT %d", state.History[0].RTT)
	}
}

// The field is optional in the REST answer: a history entry written before
// this change reads exactly like the old one.
func TestDelayHistoryRTTJSON(t *testing.T) {
	data, err := json.Marshal(C.DelayHistory{Delay: 120, RTT: 40})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"rtt":40`) {
		t.Fatalf("rtt missing from %s", data)
	}

	data, err = json.Marshal(C.DelayHistory{Delay: 120})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"rtt"`) {
		t.Fatalf("an unmeasured rtt must stay out of the payload: %s", data)
	}
}
