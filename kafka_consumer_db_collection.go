package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/revdaalex/kafka-consumer-go/config"
	failuremodel "github.com/revdaalex/kafka-consumer-go/data/failure/model"
	"github.com/revdaalex/kafka-consumer-go/data/retry/model"
	"github.com/revdaalex/kafka-consumer-go/log"
)

// kafkaConsumerDbCollection is a collection of consumers that initially consume messages from Kafka
// but then process retries from a database table instead. The failureProducer used by this collection
// should be a databaseProducer.
type kafkaConsumerDbCollection struct {
	cfg               *config.Config
	mainKafkaConsumer sarama.ConsumerGroup
	producer          *databaseProducer
	retryManager      retryManager
	handler           sarama.ConsumerGroupHandler
	handlerMap        HandlerMap
	saramaCfg         *sarama.Config
	logger            log.Logger
	connectToKafka    kafkaConnector

	// optional fields managed by setters
	maintenanceInterval time.Duration
}

type retryManager interface {
	GetBatch(ctx context.Context, topic string, sequence uint8, interval time.Duration) ([]model.Retry, error)
	MarkSuccessful(ctx context.Context, retry model.Retry) error
	MarkErrored(ctx context.Context, retry model.Retry, err error) error
	PublishFailure(ctx context.Context, f failuremodel.Failure) error
	RunMaintenance(ctx context.Context) error
}

func newKafkaConsumerDbCollection(
	cfg *config.Config,
	p *databaseProducer,
	rm retryManager,
	fch chan failuremodel.Failure,
	hm HandlerMap,
	scfg *sarama.Config,
	logger log.Logger,
	connector kafkaConnector,
) *kafkaConsumerDbCollection {
	if logger == nil {
		logger = log.NullLogger{}
	}

	return &kafkaConsumerDbCollection{
		cfg:                 cfg,
		producer:            p,
		retryManager:        rm,
		handler:             newConsumer(fch, cfg, hm, logger),
		handlerMap:          hm,
		saramaCfg:           scfg,
		logger:              logger,
		connectToKafka:      connector,
		maintenanceInterval: defaultMaintenanceInterval,
	}
}

func (cc *kafkaConsumerDbCollection) start(ctx context.Context, wg *sync.WaitGroup) error {
	topics := cc.cfg.MainTopics()
	if topics == nil || len(topics) == 0 {
		return errors.New("no Kafka topics are configured, therefore cannot start consumers")
	}

	var err error
	cc.mainKafkaConsumer, err = cc.startMainTopicConsumer(ctx, wg, topics)
	if err != nil {
		return err
	}

	for _, t := range topics {
		cc.startDbRetryProcessorsForTopic(ctx, t, cc.cfg.DBRetries[t], wg)
	}

	cc.producer.listenForFailures(ctx, wg)
	cc.periodicRetryManagerMaintenance(ctx)

	return nil
}

func (cc *kafkaConsumerDbCollection) periodicRetryManagerMaintenance(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(cc.maintenanceInterval):
				if err := cc.retryManager.RunMaintenance(ctx); err != nil {
					cc.logger.Errorf("error running maintenance in kafka consumer DB collection: %s", err)
				}
			}
		}
	}()
}

// startMainTopicConsumer starts a sarama.ConsumerGroup to consume messages from Kafka for the given main topic names
func (cc *kafkaConsumerDbCollection) startMainTopicConsumer(ctx context.Context, wg *sync.WaitGroup, topics []string) (sarama.ConsumerGroup, error) {
	cc.logger.Infof("starting Kafka consumer group for topics: '%s'", topics)

	cl, err := cc.connectToKafka(cc.cfg, cc.saramaCfg, cc.logger)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range cl.Errors() {
			cc.logger.Errorf("error occurred in consumer group Handler: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := cl.Consume(ctx, topics, cc.handler); err != nil {
					cc.logger.Errorf("error when consuming from Kafka: %s", err)
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
	}()

	return cl, nil
}

func (cc *kafkaConsumerDbCollection) startDbRetryProcessorsForTopic(ctx context.Context, topic string, retryConfig []*config.DBTopicRetry, wg *sync.WaitGroup) {
	for _, rc := range retryConfig {
		wg.Add(1)
		go func(retryConfig *config.DBTopicRetry) {
			defer wg.Done()
			timer := time.NewTimer(dbRetryPollInterval)
			for {
				select {
				case <-timer.C:
					cc.processMessagesForRetry(topic, retryConfig)
					timer.Reset(dbRetryPollInterval)
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return
				}
				time.Sleep(dbRetryPollInterval)
			}
		}(rc)
	}
}

func (cc *kafkaConsumerDbCollection) processMessagesForRetry(topic string, rc *config.DBTopicRetry) {
	// We use a standalone context here, with a timeout, this is to allow the current retry
	// processing to complete before we exit from the kafka consumer collection (see the
	// startDbRetryProcessorsForTopic method for the handling of the main context cancellation).
	// At the worst, the context timeout would be exceeded and cancelled, stopping the retry
	// batch from being processed, but it's here to prevent the whole process from becoming
	// completely locked.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	msgsForRetry, err := cc.retryManager.GetBatch(ctx, topic, rc.Sequence, rc.Interval)
	if err != nil {
		cc.logger.Errorf("error when fetching messages from the DB for retry: %s", err)
		return
	}

	h, ok := cc.handlerMap.handlerForTopic(rc.Key)
	if !ok {
		cc.logger.Errorf("no handler found for topic key '%s'", rc.Key)
		return
	}

	for _, msg := range msgsForRetry {
		saramaMsg := msg.ToSaramaConsumerMessage()
		if err = h(ctx, saramaMsg); err != nil {
			cc.logger.Errorf("error processing retried message from DB: %s", err)
			if repoErr := cc.retryManager.MarkErrored(ctx, msg, err); repoErr != nil {
				cc.logger.Errorf("error marking retried message as errored in the DB: %s", repoErr)
			}
		} else {
			cc.logger.Infof("successfully processed retried message from topic '%s' with original partition %d and offset %d", topic, msg.KafkaPartition, msg.KafkaOffset)
			if err = cc.retryManager.MarkSuccessful(ctx, msg); err != nil {
				cc.logger.Errorf("error marking retried message as successful in the DB: %s", err)
			}
		}
	}
}

func (cc *kafkaConsumerDbCollection) close() {
	if cc.mainKafkaConsumer == nil {
		return
	}

	if err := cc.mainKafkaConsumer.Close(); err != nil {
		cc.logger.Errorf("error occurred closing the main Kafka consumer: %w", err)
	}
	cc.mainKafkaConsumer = nil
}

func (cc *kafkaConsumerDbCollection) setMaintenanceInterval(duration time.Duration) {
	cc.maintenanceInterval = duration
}
