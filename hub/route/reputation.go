package route

import (
	"strconv"

	"github.com/metacubex/mihomo/component/reputation"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func reputationRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/history", getReputationHistory)
	return r
}

// getReputationHistory returns the recent score changes of one node: what the
// store thought of it before and after every dial, probe and silent flow. The
// answer is a debug view, so the history is bounded no matter what the caller
// asks for.
func getReputationHistory(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	limit := reputation.HistoryLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}
		limit = parsed
	}
	if limit > reputation.HistoryLimit {
		limit = reputation.HistoryLimit
	}

	store := reputation.Default()
	render.JSON(w, r, render.M{
		"name":   name,
		"score":  store.Score(name),
		"events": store.History(name, limit),
		"limit":  limit,
	})
}
