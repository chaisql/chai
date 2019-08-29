package parser

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	s, err := Parse("wesh", []byte("sElEct FroM f北P京_市_"), Debug(true))
	require.NoError(t, err)
	spew.Dump(s)
}
