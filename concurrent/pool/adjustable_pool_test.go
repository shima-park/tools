package pool

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAdjuestablePool(t *testing.T) {
	p, err := NewAdjustablePool(func(v interface{}) (Worker, error) {
		return func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				time.Sleep(time.Millisecond)
			}
		}, nil
	})
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		err = p.Add(1, i)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second)
	assert.Equal(t, 10, p.Running())
	assert.Equal(t, 0, p.Exiting())
	fmt.Println("Running:", p.Running(), "Exiting:", p.Exiting())

	time.Sleep(time.Second)

	err = p.Reduce(9)
	assert.Nil(t, err)
	fmt.Println("Running:", p.Running(), "Exiting:", p.Exiting())
	assert.GreaterOrEqual(t, 9, p.Exiting())
	time.Sleep(time.Second)
	assert.Equal(t, 1, p.Running())

	time.Sleep(time.Second)

	p.Stop()
	assert.GreaterOrEqual(t, 0, p.Exiting())
	assert.Equal(t, 0, p.Running())
	fmt.Println("Running:", p.Running(), "Exiting:", p.Exiting())
}
