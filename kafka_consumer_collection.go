package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/revdaalex/kafka-consumer-go/config"
	"github.com/revdaalex/kafka-consumer-go/data/failure/model"
	"github.com/revdaalex/kafka-consumer-go/log"
)

type failureProducer interface {
	listenForFailures(ctx context.Context, wg *sync.WaitGroup)
}

type kafkaConsumerCollection struct {
	cfg            *config.Config
	consumers      []sarama.ConsumerGroup
	producer       failureProducer
	handler        sarama.ConsumerGroupHandler
	saramaCfg      *sarama.Config
	logger         log.Logger
	connectToKafka kafkaConnector
}

func newKafkaConsumerCollection(
	cfg *config.Config,
	p failureProducer,
	fch chan model.Failure,
	hm HandlerMap,
	scfg *sarama.Config,
	logger log.Logger,
	connector kafkaConnector,
) *kafkaConsumerCollection {
	if logger == nil {
		logger = log.NullLogger{}
	}

	return &kafkaConsumerCollection{
		cfg:            cfg,
		consumers:      []sarama.ConsumerGroup{},
		producer:       p,
		handler:        newConsumer(fch, cfg, hm, logger),
		saramaCfg:      scfg,
		logger:         logger,
		connectToKafka: connector,
	}
}

func (cc *kafkaConsumerCollection) start(ctx context.Context, wg *sync.WaitGroup) error {
	topics := cc.cfg.ConsumableTopics
	if topics == nil {
		return errors.New("no Kafka topics are configured, therefore cannot start consumers")
	}

	for _, t := range topics {
		if !t.IsMainTopic && len(cc.cfg.RetryHost) > 0 {
			cc.cfg.Host = cc.cfg.RetryHost
		}

		group, err := cc.startConsumerGroup(ctx, wg, t)
		if err != nil {
			return err
		}
		cc.consumers = append(cc.consumers, group)
	}
	cc.producer.listenForFailures(ctx, wg)

	return nil
}

func (cc *kafkaConsumerCollection) close() {
	for _, c := range cc.consumers {
		if err := c.Close(); err != nil {
			cc.logger.Errorf("error occurred closing a Kafka consumer: %w", err)
		}
	}
	cc.consumers = []sarama.ConsumerGroup{}
}

func (cc *kafkaConsumerCollection) startConsumerGroup(ctx context.Context, wg *sync.WaitGroup, topic *config.KafkaTopic) (sarama.ConsumerGroup, error) {
	cc.logger.Infof("starting Kafka consumer group for '%s'", topic.Name)

	cl, err := cc.connectToKafka(cc.cfg, cc.saramaCfg, cc.logger)
	if err != nil {
		return nil, err
	}

	cc.startConsumer(cl, ctx, wg, topic)

	return cl, nil
}

func (cc *kafkaConsumerCollection) startConsumer(cl sarama.ConsumerGroup, ctx context.Context, wg *sync.WaitGroup, topic *config.KafkaTopic) {
	go func() {
		for err := range cl.Errors() {
			cc.logger.Errorf("error occurred in consumer group Handler: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(topic.Delay)
		for {
			select {
			case <-timer.C:
				if err := cl.Consume(ctx, []string{topic.Name}, cc.handler); err != nil {
					cc.logger.Errorf("error when consuming from Kafka: %s", err)
				}
				if ctx.Err() != nil {
					timer.Stop()
					return
				}
				timer.Reset(topic.Delay)
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
	}()
}
