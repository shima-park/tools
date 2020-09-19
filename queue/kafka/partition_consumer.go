package kafka

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	"go.uber.org/atomic"
	"sync"
)

type PartitionConsumer struct {
	config PartitionConsumerConfig

	ctx      context.Context
	cancel   context.CancelFunc
	isClosed *atomic.Bool
}

type PartitionConsumerConfig struct {
	Addrs      []string
	Topic      string
	Partitions []int32
	Offset     int64 // 0~n, OffsetOldest, OffsetNewest
}

func NewPartitionConsumer(config PartitionConsumerConfig) *PartitionConsumer {
	return &PartitionConsumer{
		config:   config,
		isClosed: atomic.NewBool(false),
	}
}

type PartitionConsumerHandler func(*sarama.ConsumerMessage) (isContinue bool)

// Start 启动消费对应的partitions，如果传入的partitions为空，
// 则会通过接口获取所有partitions，并启动对应数量的consumer去消费
// 当handle返回false，则停止消费，退出循环，并close掉consumer
func (c *PartitionConsumer) Start(pctx context.Context, handle PartitionConsumerHandler) error {
	if pctx == nil {
		pctx = context.Background()
	}
	c.ctx, c.cancel = context.WithCancel(pctx)

	if handle == nil {
		return errors.New("The PartitionConsumerHandler cannot be nil")
	}

	master, err := sarama.NewConsumer(c.config.Addrs, nil)
	if err != nil {
		return err
	}

	partitions := c.config.Partitions
	if len(partitions) == 0 {
		partitions, err = master.Partitions(c.config.Topic)
		if err != nil {
			return err
		}
	}

	var consumers []sarama.PartitionConsumer
	for _, partition := range partitions {
		consumer, err := master.ConsumePartition(
			c.config.Topic, partition, c.config.Offset)
		if err != nil {
			return err
		}
		consumers = append(consumers, consumer)
	}

	var wg sync.WaitGroup
	for i := range consumers {
		consumer := consumers[i]
		wg.Add(1)
		go func() {
			defer func() {
				consumer.Close()
				wg.Done()
			}()

			for {
				select {
				case msg := <-consumer.Messages():
					if !handle(msg) {
						return
					}
				case <-c.ctx.Done():
					return
				}
			}
		}()
	}
	wg.Wait()

	return nil
}

func (c *PartitionConsumer) Stop() {
	if !c.isClosed.CAS(false, true) {
		return
	}

	if c.cancel != nil {
		c.cancel()
	}
}
