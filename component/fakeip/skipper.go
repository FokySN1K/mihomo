package fakeip

import (
	"slices"

	C "github.com/metacubex/mihomo/constant"
)

const (
	UseFakeIP = "fake-ip"
	UseRealIP = "real-ip"
)

// directTarget leaves the connection on the local network.
const directTarget = "DIRECT"

// passTargets hand the decision to the following rules, the tunnel does the same.
var passTargets = []string{"PASS", "PASS-RULE"}

type Skipper struct {
	Rules  []C.Rule
	Router []C.Rule
	Host   []C.DomainMatcher
	Mode   C.FilterMode
}

// ShouldSkipped return if domain should be skipped
func (p *Skipper) ShouldSkipped(domain string) bool {
	switch p.Mode {
	case C.FilterRule:
		return p.shouldSkippedByRule(domain)
	case C.FilterRoute:
		return p.shouldSkippedByRoute(domain)
	}

	should := p.shouldSkipped(domain)
	if p.Mode == C.FilterWhiteList {
		return !should
	}
	return should
}

func (p *Skipper) shouldSkippedByRule(domain string) bool {
	metadata := &C.Metadata{Host: domain}
	for _, rule := range p.Rules {
		if matched, action := rule.Match(metadata, C.RuleMatchHelper{}); matched {
			return action == UseRealIP
		}
	}
	return false
}

// shouldSkippedByRoute mirrors the tunnel on the domain only: a destination that
// leaves by DIRECT gets a real IP, everything else is proxied and needs the fake
// one to keep the domain available for the rule matching. IP and process rules
// never match here, because the DNS layer knows neither the address nor the app.
func (p *Skipper) shouldSkippedByRoute(domain string) bool {
	metadata := &C.Metadata{Host: domain}
	for _, rule := range p.Router {
		if wrapper, ok := rule.(C.RuleWrapper); ok {
			if wrapper.IsDisabled() {
				continue
			}
			// unwrapped matching keeps the hit counters for real connections
			rule = wrapper.Unwrap()
		}

		matched, action := rule.Match(metadata, C.RuleMatchHelper{})
		if !matched {
			continue
		}
		if slices.Contains(passTargets, action) {
			continue
		}
		return action == directTarget
	}
	return true
}

func (p *Skipper) shouldSkipped(domain string) bool {
	for _, matcher := range p.Host {
		if matcher.MatchDomain(domain) {
			return true
		}
	}
	return false
}
