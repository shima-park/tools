package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/atomic"
)

type PartitionConsumer struct {
	config PartitionConsumerConfig

	master    sarama.Consumer
	consumers []sarama.PartitionConsumer

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

func (c *PartitionConsumer) Start(handler func(*sarama.ConsumerMessage)) error {
	var err error
	c.master, err = sarama.NewConsumer(c.config.Addrs, nil)
	if err != nil {
		return err
	}

	if len(c.config.Partitions) == 0 {
		c.config.Partitions, err = c.master.Partitions(c.config.Topic)
		if err != nil {
			return err
		}
	}

	for _, partition := range c.config.Partitions {
		var consumer sarama.PartitionConsumer
		consumer, err = c.master.ConsumePartition(c.config.Topic, partition, c.config.offset)
		if err != nil {
			return err
		}
		c.consumers = append(c.consumers, consumer)

		c.wg.Add(1)
		go func() {
			defer func() {
				consumer.Close()
				c.wg.Done()
			}()

			for {
				select {
				case msg := <-consumer.Messages():
					handler(msg)
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

	c.master.Close()

	c.wg.Wait()
}
