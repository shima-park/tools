package excel

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateColumnPrefix(t *testing.T) {
	var tests = []struct {
		Case     int
		Expected string
	}{
		{0, "A"},
		{25, "Z"},
		{26, "AA"},
		{700, "ZY"},
		{702, "AAA"},
	}

	for _, test := range tests {
		actual := generateColumnPrefix(test.Case)
		fmt.Println(test.Case, actual)
		assert.Equal(t, test.Expected, actual)
	}
}
