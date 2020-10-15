package bloom

import (
	"context"
	"github.com/go-redis/redis/v8"
)

// BloomFilter doc: https://oss.redislabs.com/redisbloom/Bloom_Commands/
type BloomFilter struct {
	client *redis.Client
}

func NewBloomFilter(client *redis.Client) *BloomFilter {
	return &BloomFilter{
		client: client,
	}
}

/*
Reserve
key : The key under which the filter is to be found

error_rate : The desired probability for false positives.
This should be a decimal value between 0 and 1. For example,
for a desired false positive rate of 0.1% (1 in 1000), error_rate should be set to 0.001.
The closer this number is to zero, the greater the memory consumption per item and the more CPU usage per operation.

capacity : The number of entries you intend to add to the filter.
Performance will begin to degrade after adding more items than this number.
The actual degradation will depend on how far the limit has been exceeded.
Performance will degrade linearly as the number of entries grow exponentially.
capacity大小不能大于4294967293，目前作者的实现超过此值会报错
*/
func (f *BloomFilter) Reserve(ctx context.Context, key string, errorRate float64, capacity int64) error {
	cmd := redis.NewStatusCmd(ctx, "BF.RESERVE", key, errorRate, capacity)
	f.client.Process(ctx, cmd)
	return cmd.Err()
}

// Add Adds an item to the Bloom Filter, creating the filter if it does not yet exist.
// key : The name of the filter
// item : The item to add
func (f *BloomFilter) Add(ctx context.Context, key, item string) (bool, error) {
	cmd := redis.NewBoolCmd(ctx, "BF.ADD", key, item)
	f.client.Process(ctx, cmd)
	return cmd.Result()
}

// MAdd Adds one or more items to the Bloom Filter, creating the filter if it does not yet exist. This command operates identically to BF.ADD except it allows multiple inputs and returns multiple values.
// key : The name of the filter
// items : One or more items to add
func (f *BloomFilter) MAdd(ctx context.Context, key string, items ...string) ([]bool, error) {
	args := []interface{}{"BF.MADD", key}
	for _, item := range items {
		args = append(args, item)
	}
	cmd := redis.NewBoolSliceCmd(ctx, args...)
	f.client.Process(ctx, cmd)
	return cmd.Result()
}

// Exists Determines whether an item may exist in the Bloom Filter or not.
// key : the name of the filter
// item : the item to check for
func (f *BloomFilter) Exists(ctx context.Context, key, item string) (bool, error) {
	cmd := redis.NewBoolCmd(ctx, "BF.EXISTS", key, item)
	f.client.Process(ctx, cmd)
	return cmd.Result()
}

// Determines if one or more items may exist in the filter or not.
// key : name of the filter
// items : one or more items to check
func (f *BloomFilter) MEXISTS(ctx context.Context, key string, items ...string) ([]bool, error) {
	args := []interface{}{"BF.MEXISTS", key}
	for _, item := range items {
		args = append(args, item)
	}
	cmd := redis.NewBoolSliceCmd(ctx, args...)
	f.client.Process(ctx, cmd)
	return cmd.Result()
}
