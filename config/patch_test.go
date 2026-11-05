package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetProxyNameListReturnsCopy(t *testing.T) {
	SetProxyNameList([]string{"A", "B"})

	list := GetProxyNameList()
	require.Equal(t, []string{"A", "B"}, list)

	list[0] = "changed"
	require.Equal(t, []string{"A", "B"}, GetProxyNameList(), "the caller must not reach the shared list")
}
