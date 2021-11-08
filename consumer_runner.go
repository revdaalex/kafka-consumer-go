package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"

	"github.com/inviqa/kafka-consumer-go/config"
	"github.com/inviqa/kafka-consumer-go/data"
	"github.com/inviqa/kafka-consumer-go/data/failure/model"
	"github.com/inviqa/kafka-consumer-go/data/retry"
	"github.com/inviqa/kafka-consumer-go/log"
)

func Start(cfg *config.Config, ctx context.Context, hs HandlerMap, logger log.Logger) error {
	if logger == nil {
		logger = log.NullLogger{}
	}

	wg := &sync.WaitGroup{}
	fch := make(chan model.Failure)
	srmCfg := config.NewSaramaConfig(cfg.TLSEnable, cfg.TLSSkipVerifyPeer)

	var cons collection
	var err error

	if cfg.UseDBForRetryQueue {
		cons, err = setupKafkaConsumerDbCollection(cfg, logger, fch, hs, srmCfg)
		if err != nil {
			return err
		}
	} else {
		kafkaProducer, err := newKafkaFailureProducerWithDefaults(cfg, fch, logger)
		if err != nil {
			return fmt.Errorf("could not start Kafka failure producer: %w", err)
		}
		cons = newKafkaConsumerCollection(cfg, kafkaProducer, fch, hs, srmCfg, logger)
	}

	if err := cons.Start(ctx, wg); err != nil {
		return fmt.Errorf("unable to start consumers: %w", err)
	}
	defer cons.Close()

	logger.Info("kafka consumer started")

	wg.Wait()

	return nil
}

func setupKafkaConsumerDbCollection(cfg *config.Config, logger log.Logger, fch chan model.Failure, hs HandlerMap, srmCfg *sarama.Config) (collection, error) {
	db, err := data.NewDB(cfg.GetDBConnectionString(), logger)
	if err != nil {
		return nil, fmt.Errorf("could not connect to DB: %w", err)
	}

	repo := retry.NewManagerWithDefaults(cfg.DBRetries, db)
	dbProducer := newDatabaseProducer(repo, fch, logger)
	cons := newKafkaConsumerDbCollection(cfg, dbProducer, repo, fch, hs, srmCfg, logger, defaultKafkaConnector)
	cons.setMaintenanceInterval(cfg.MaintenanceInterval)

	return cons, nil
}
