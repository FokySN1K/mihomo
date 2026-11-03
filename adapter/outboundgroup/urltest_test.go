package outboundgroup

import (
	"testing"

	"github.com/metacubex/mihomo/common/structure"

	"github.com/stretchr/testify/require"
)

func TestURLTestToleranceOption(t *testing.T) {
	group, err := NewURLTest(GroupCommonOption{Name: "auto"}, URLTestOption{}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Equal(t, defaultURLTestTolerance, group.tolerance, "tolerance defaults to 150 ms")

	zero := uint16(0)
	group, err = NewURLTest(GroupCommonOption{Name: "auto"}, URLTestOption{Tolerance: &zero}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Zero(t, group.tolerance, "tolerance 0 disables the hysteresis")

	wide := uint16(400)
	group, err = NewURLTest(GroupCommonOption{Name: "auto"}, URLTestOption{Tolerance: &wide}, newPoolEmptyFallback(), nil)
	require.NoError(t, err)
	require.Equal(t, wide, group.tolerance)
}

// TestToleranceParsesFromConfig guards the pointer fields: an unset key must
// stay distinguishable from an explicit zero, which turns the hysteresis off.
func TestToleranceParsesFromConfig(t *testing.T) {
	decoder := structure.NewDecoder(structure.Option{TagName: "group", WeaklyTypedInput: true})

	urlTestOption := URLTestOption{}
	require.NoError(t, decoder.Decode(map[string]any{"tolerance": 220}, &urlTestOption))
	require.NotNil(t, urlTestOption.Tolerance)
	require.Equal(t, uint16(220), *urlTestOption.Tolerance)

	poolOption := URLTestPoolOption{}
	require.NoError(t, decoder.Decode(map[string]any{"tolerance": 0}, &poolOption))
	require.NotNil(t, poolOption.Tolerance)
	require.Zero(t, *poolOption.Tolerance)

	unset := URLTestOption{}
	require.NoError(t, decoder.Decode(map[string]any{}, &unset))
	require.Nil(t, unset.Tolerance)
}
