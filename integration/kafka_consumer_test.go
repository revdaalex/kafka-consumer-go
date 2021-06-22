// +build integration

package integration

import (
	"testing"

	"github.com/inviqa/kafka-consumer-go/integration/kafka"
)

func TestMessagesAreConsumedFromKafka(t *testing.T) {
	publishTestMessageToKafka(kafka.TestMessage{})

	handler := kafka.NewTestConsumerHandler()

	consumeFromKafkaUntil(func(doneCh chan<- bool) {
		for {
			if len(handler.RecvdMessages) == 1 {
				doneCh <- true
			}
		}
	}, handler.Handle)

	if len(handler.RecvdMessages) != 1 {
		t.Errorf("expected 1 message to be received by handler, received %d", len(handler.RecvdMessages))
	}
}

func TestMessagesAreConsumedFromKafka_withError(t *testing.T) {
	publishTestMessageToKafka(kafka.TestMessage{})

	handler := kafka.NewTestConsumerHandler()
	handler.WillFail()

	consumeFromKafkaUntil(func(doneCh chan<- bool) {
		for {
			if len(handler.RecvdMessages) == 2 {
				doneCh <- true
			}
		}
	}, handler.Handle)

	if len(handler.RecvdMessages) != 2 {
		t.Errorf("expected 2 messages to be received by handler, received %d", len(handler.RecvdMessages))
	}
}