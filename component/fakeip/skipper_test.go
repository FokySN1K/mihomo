package fakeip

import (
	"testing"

	"github.com/metacubex/mihomo/component/trie"
	C "github.com/metacubex/mihomo/constant"
	RC "github.com/metacubex/mihomo/rules/common"
	RW "github.com/metacubex/mihomo/rules/wrapper"

	"github.com/stretchr/testify/assert"
)

func TestSkipper_BlackList(t *testing.T) {
	var builder trie.DomainSetBuilder
	assert.NoError(t, builder.Insert("example.com"))
	assert.False(t, builder.IsEmpty())
	skipper := &Skipper{
		Host: []C.DomainMatcher{builder.Build()},
	}
	assert.True(t, skipper.ShouldSkipped("example.com"))
	assert.False(t, skipper.ShouldSkipped("foo.com"))
	assert.False(t, skipper.shouldSkipped("baz.com"))
}

func TestSkipper_WhiteList(t *testing.T) {
	var builder trie.DomainSetBuilder
	assert.NoError(t, builder.Insert("example.com"))
	assert.False(t, builder.IsEmpty())
	skipper := &Skipper{
		Host: []C.DomainMatcher{builder.Build()},
		Mode: C.FilterWhiteList,
	}
	assert.False(t, skipper.ShouldSkipped("example.com"))
	assert.True(t, skipper.ShouldSkipped("foo.com"))
	assert.True(t, skipper.ShouldSkipped("baz.com"))
}

func TestSkipper_Rule(t *testing.T) {
	skipper := &Skipper{
		Mode: C.FilterRule,
		Rules: []C.Rule{
			RC.NewDomainSuffix("example.com", UseRealIP),
			RC.NewMatch(UseFakeIP),
		},
	}
	assert.True(t, skipper.ShouldSkipped("example.com"))
	assert.False(t, skipper.ShouldSkipped("foo.com"))
}

func TestSkipper_RouteFollowsDirectRule(t *testing.T) {
	skipper := &Skipper{
		Mode: C.FilterRoute,
		Router: []C.Rule{
			RC.NewDomainSuffix("example.com", "DIRECT"),
			RC.NewDomainSuffix("proxied.com", "Proxy"),
			RC.NewMatch("Proxy"),
		},
	}
	assert.True(t, skipper.ShouldSkipped("example.com"))
	assert.False(t, skipper.ShouldSkipped("proxied.com"))
	assert.False(t, skipper.ShouldSkipped("foo.com"))
}

func TestSkipper_RouteWithoutMatchIsDirect(t *testing.T) {
	skipper := &Skipper{
		Mode: C.FilterRoute,
		Router: []C.Rule{
			RC.NewDomainSuffix("proxied.com", "Proxy"),
		},
	}
	// the tunnel falls back to DIRECT when no rule matches
	assert.True(t, skipper.ShouldSkipped("foo.com"))
	assert.False(t, skipper.ShouldSkipped("proxied.com"))
}

func TestSkipper_RouteContinuesOnPass(t *testing.T) {
	skipper := &Skipper{
		Mode: C.FilterRoute,
		Router: []C.Rule{
			RC.NewDomain("example.com", "PASS"),
			RC.NewMatch("DIRECT"),
		},
	}
	assert.True(t, skipper.ShouldSkipped("example.com"))
}

func TestSkipper_RouteSkipsDisabledRule(t *testing.T) {
	disabled := RW.NewRuleWrapper(RC.NewDomainSuffix("example.com", "DIRECT"))
	disabled.SetDisabled(true)
	skipper := &Skipper{
		Mode: C.FilterRoute,
		Router: []C.Rule{
			disabled,
			RC.NewMatch("Proxy"),
		},
	}
	assert.False(t, skipper.ShouldSkipped("example.com"))
}
