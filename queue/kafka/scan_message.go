package kafka

import (
	"context"
	"github.com/Shopify/sarama"
)

type Message struct {
	Message *sarama.ConsumerMessage
	Err     error
}

func (m Message) String() string {
	if m.Message != nil {
		return string(m.Message.Value)
	}
	return ""
}

// ScanMessage
// offset Should be OffsetNewest or OffsetOldest. Defaults to OffsetNewest.
func ScanMessage(ctx context.Context, addrs, topics []string, groupID string, offset int64) chan Message {
	if ctx == nil {
		ctx = context.Background()
	}

	ch := make(chan Message, 1)

	go func() {
		config := sarama.NewConfig()
		config.Version = sarama.V2_0_0_0
		config.Consumer.Return.Errors = true
		config.Consumer.Offsets.Initial = offset
		c := NewGroupConsumer(GroupConsumerConfig{
			Addrs:         addrs,
			Topics:        topics,
			ConsumerGroup: groupID,
			Config:        config,
		})
		defer c.Stop()

		handle := func(msg *sarama.ConsumerMessage) (isAck, isConitnue bool) {
			select {
			case <-ctx.Done():
				return false, false
			case ch <- Message{Message: msg}:
				return true, true
			}
		}

		errHandle := func(err error) {
			select {
			case <-ctx.Done():
				return
			case ch <- Message{Err: err}:

			}
		}

		err := c.Start(ctx, errHandle, handle)
		defer close(ch)
		if err != nil {
			ch <- Message{Err: err}
			return
		}
	}()
	return ch
}
