package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/atomic"
	"golang.org/x/net/context"
)

type GroupConsumer struct {
	config GroupConsumerConfig

	group sarama.ConsumerGroup

	wg       *sync.WaitGroup
	done     chan struct{}
	isClosed *atomic.Bool
}

type GroupConsumerConfig struct {
	Addrs         []string
	Topics        []string
	ConsumerGroup string
	Config        *sarama.Config
}

func NewGroupConsumer(config GroupConsumerConfig) *GroupConsumer {
	return &GroupConsumer{
		config:   config,
		wg:       &sync.WaitGroup{},
		isClosed: atomic.NewBool(false),
		done:     make(chan struct{}),
	}
}

func (c *GroupConsumer) Start(errHandle func(error), handle func(*sarama.ConsumerMessage) (isAck bool)) error {
	if c.config.Config == nil {
		c.config.Config = sarama.NewConfig()
		c.config.Config.Version = sarama.V2_0_0_0
		c.config.Config.Consumer.Return.Errors = true
	}

	var err error
	c.group, err = sarama.NewConsumerGroup(c.config.Addrs, c.config.ConsumerGroup, c.config.Config)
	if err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for err := range c.group.Errors() {
			if errHandle != nil {
				errHandle(err)
			}
		}
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for !c.isClosed.Load() {
			handler := consumerGroupHandler{handle: handle}

			err := c.group.Consume(context.Background(), c.config.Topics, handler)
			if err != nil && errHandle != nil {
				errHandle(err)
			}
		}
	}()

	return nil
}

func (c *GroupConsumer) Stop() {
	if !c.isClosed.CAS(false, true) {
		return
	}

	close(c.done)

	c.group.Close()

	c.wg.Wait()
}

type consumerGroupHandler struct {
	handle func(*sarama.ConsumerMessage) bool
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if h.handle(msg) {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}
