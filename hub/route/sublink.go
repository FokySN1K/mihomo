package route

import (
	"time"

	"github.com/metacubex/mihomo/component/reputation"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

// The SNKP telemetry asks the core for two snapshots the plain clash API does
// not expose: what every outbound node carried and how the nodes behaved when
// real traffic went through them. Both are read only; the app turns the totals
// into deltas before reporting and keeps the endpoints free of side effects.

type reputationResponse struct {
	GeneratedAt time.Time                  `json:"generated_at"`
	Nodes       []reputation.SnapshotEntry `json:"nodes"`
}

func getReputation(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, reputationResponse{
		GeneratedAt: time.Now(),
		Nodes:       reputation.Default().Snapshot(),
	})
}

type nodeTrafficResponse struct {
	GeneratedAt time.Time             `json:"generated_at"`
	Nodes       []statistic.NodeUsage `json:"nodes"`
}

func getNodeTraffic(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, nodeTrafficResponse{
		GeneratedAt: time.Now(),
		Nodes:       statistic.DefaultManager.NodeSnapshot(),
	})
}
