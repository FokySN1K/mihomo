package adapter

// timedOut follows hub/route's getProxyDelay, which answers 504 rather than 503
// once the test's context is done. rtt is the time the dial took, the part of
// the delay the node itself is responsible for; a test that never reached the
// proxy reports zero.
type UrlTestCheck func(url string, name string, delay uint16, rtt uint16, timedOut bool)

// UrlTestStartCheck reports a probe that has just begun. A group check decides
// on its own which members are in flight, so a host that shows what is being
// measured needs the start of the probe, not only its result.
type UrlTestStartCheck func(url string, name string)

var (
	UrlTestHook      UrlTestCheck
	UrlTestStartHook UrlTestStartCheck
)
