package outboundgroup

// The reason a computed group routes through its current member. The values
// are part of the REST contract: a host renders them as they are, and an
// unknown value must render as is instead of breaking the group card.
const (
	// NowReasonFixed means the group routes through the member the user
	// pinned. The pin is a decision, not a health hint: it holds even when a
	// check marks the member out of play, and the marks are reported on their
	// own fields so a host can show the risk on the member.
	NowReasonFixed = "fixed"
	// NowReasonFixedUnavailable means a member is pinned, but it has left the
	// member list, so the group cannot dial it and routes through another
	// member.
	NowReasonFixedUnavailable = "fixed-unavailable"
	// NowReasonTolerance means a faster member exists, but the group kept the
	// current one because the gain is inside the tolerance band.
	NowReasonTolerance = "tolerance"
	// NowReasonPromotion means a faster member was measured, but its
	// measurement is stale and the group is probing it before promotion.
	NowReasonPromotion = "promotion"
	// NowReasonBest means the group chose its best measured alive member.
	NowReasonBest = "best"
	// NowReasonLastGood means nothing is measured alive, so the group fell
	// back to the member that carried traffic last.
	NowReasonLastGood = "last-good"
	// NowReasonEmpty means the group has no usable member and falls back to
	// its empty-fallback proxy or an unmeasured guess.
	NowReasonEmpty = "empty"
)

// fixedActive reports whether the pinned member is the one the group actually
// routes through. A pin that is set but not active stays in `fixed`, so a host
// can tell the user why the route does not follow it.
func fixedActive(fixed, now string) bool {
	return fixed != "" && now == fixed
}
