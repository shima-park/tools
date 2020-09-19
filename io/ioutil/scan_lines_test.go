package ioutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScanLines(t *testing.T) {
	for line := range ScanFileLines(context.Background(), "/etc/hosts") {
		t.Log("Line:", line.String())
		assert.Nil(t, line.Err)
		assert.NotEmpty(t, line.String())
	}
}
