package route

import (
	"encoding/json"
	"testing"

	"github.com/metacubex/mihomo/component/reputation"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func TestGetReputationHistory(t *testing.T) {
	store := reputation.Default()
	store.DialFailed("route-test-node")

	req := httptest.NewRequest(http.MethodGet, "/reputation/history?name=route-test-node&limit=5", nil)
	rec := httptest.NewRecorder()
	getReputationHistory(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		Name   string             `json:"name"`
		Score  int                `json:"score"`
		Events []reputation.Event `json:"events"`
		Limit  int                `json:"limit"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unreadable answer: %v", err)
	}
	if body.Name != "route-test-node" {
		t.Fatalf("unexpected name %q", body.Name)
	}
	if body.Limit != 5 {
		t.Fatalf("unexpected limit %d", body.Limit)
	}
	if body.Score >= 0 {
		t.Fatalf("a failed dial must leave a negative score, got %d", body.Score)
	}
	if len(body.Events) == 0 {
		t.Fatal("the answer carries no events")
	}
}

func TestGetReputationHistoryRequiresName(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/reputation/history", nil)
	rec := httptest.NewRecorder()
	getReputationHistory(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status %d", rec.Code)
	}
}

func TestGetReputationHistoryRejectsBadLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/reputation/history?name=x&limit=zero", nil)
	rec := httptest.NewRecorder()
	getReputationHistory(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status %d", rec.Code)
	}
}
