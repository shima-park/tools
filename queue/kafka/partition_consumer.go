package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/atomic"
)

type PartitionConsumer struct {
	config PartitionConsumerConfig

	wg       *sync.WaitGroup
	done     chan struct{}
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
		wg:       &sync.WaitGroup{},
		isClosed: atomic.NewBool(false),
		done:     make(chan struct{}),
	}
}

// Start 启动消费对应的partitions，如果传入的partitions为空，
// 则会通过接口获取所有partitions，并启动对应数量的consumer去消费
// 当handle返回false，则停止消费，退出循环，并close掉consumer
func (c *PartitionConsumer) Start(handle func(*sarama.ConsumerMessage) (isContinue bool)) error {
	master, err := sarama.NewConsumer(c.config.Addrs, nil)
	if err != nil {
		return err
	}

	if len(c.config.Partitions) == 0 {
		c.config.Partitions, err = master.Partitions(c.config.Topic)
		if err != nil {
			return err
		}
	}

	for _, partition := range c.config.Partitions {
		consumer, err := master.ConsumePartition(c.config.Topic, partition, c.config.Offset)
		if err != nil {
			return err
		}

		c.wg.Add(1)
		go func() {
			defer func() {
				consumer.Close()
				c.wg.Done()
			}()

			for {
				select {
				case msg := <-consumer.Messages():
					if !handle(msg) {
						return
					}
				case <-c.done:
					return
				}
			}
		}()
	}

	return nil
}

func (c *PartitionConsumer) Stop() {
	if !c.isClosed.CAS(false, true) {
		return
	}

	close(c.done)

	c.wg.Wait()
}
