package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestScanMessage(t *testing.T) {
	startTime := time.Now()
	defer func(startTime time.Time) {
		t.Log(time.Since(startTime))
	}(startTime)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-time.After(time.Second * 10)
		cancel()
	}()

	var i int
	for msg := range ScanMessage(ctx,
		[]string{"localhost:9092"},
		[]string{"test_topic"},
		"test_scan_message",
		sarama.OffsetNewest,
	) {
		if msg.Err != nil {
			t.Fatal(msg.Err)
			break
		}
		t.Log(i, msg.String())
		i++
	}
}
