package pdf

import (
	"testing"

	"fmt"

	"github.com/stretchr/testify/assert"
)

func TestExtractPages(t *testing.T) {
	pages, err := ExtractPages("/Users/liuxingwang/600031_独立董事意见.pdf")
	assert.Nil(t, err)
	fmt.Println(Pages(pages).String())
}
