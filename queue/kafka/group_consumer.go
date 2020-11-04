package kafka

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"go.uber.org/atomic"
)

type GroupConsumer struct {
	config GroupConsumerConfig

	ctx    context.Context
	cancel context.CancelFunc

	isClosed *atomic.Bool
}

type GroupConsumerConfig struct {
	Addrs         []string
	Topics        []string
	ConsumerGroup string
	Config        *sarama.Config
}

func NewGroupConsumer(config GroupConsumerConfig) *GroupConsumer {
	if config.Config == nil {
		config.Config = sarama.NewConfig()
		config.Config.Version = sarama.V2_0_0_0
		config.Config.Consumer.Return.Errors = true
	}

	return &GroupConsumer{
		config:   config,
		isClosed: atomic.NewBool(false),
	}
}

func (c *GroupConsumer) Start(pctx context.Context, errHandle func(error), handle ConsumerGroupHandler) error {
	if handle == nil {
		return errors.New("The ConsumerGroupHandler cannot be nil")
	}

	if pctx == nil {
		pctx = context.Background()
	}
	c.ctx, c.cancel = context.WithCancel(pctx)

	group, err := sarama.NewConsumerGroup(c.config.Addrs, c.config.ConsumerGroup, c.config.Config)
	if err != nil {
		return err
	}
	defer group.Close()

	go func() {
		for err := range group.Errors() {
			if errHandle != nil {
				errHandle(err)
			}
		}
	}()

	for !c.isClosed.Load() {
		select {
		case <-c.ctx.Done():
			return nil
		default:
			handler := consumerGroupHandler{
				handle: handle,
			}

			err := group.Consume(c.ctx, c.config.Topics, handler)
			if err != nil && errHandle != nil {
				errHandle(err)
			}
		}
	}

	return nil
}

func (c *GroupConsumer) Stop() {
	if !c.isClosed.CAS(false, true) {
		return
	}

	if c.cancel != nil {
		c.cancel()
	}
}

type ConsumerGroupHandler func(*sarama.ConsumerMessage) (isAck, isContinue bool)

type consumerGroupHandler struct {
	handle ConsumerGroupHandler
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg != nil {
			continue
		}
		isAck, isContinue := h.handle(msg)
		if isAck {
			sess.MarkMessage(msg, "")
		}
		if !isContinue {
			return nil
		}
	}
	return nil
}
