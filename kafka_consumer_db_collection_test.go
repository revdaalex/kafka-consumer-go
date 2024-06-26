package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-test/deep"

	"github.com/revdaalex/kafka-consumer-go/config"
	"github.com/revdaalex/kafka-consumer-go/data/failure/model"
	"github.com/revdaalex/kafka-consumer-go/log"
	"github.com/revdaalex/kafka-consumer-go/test/saramatest"
)

func TestNewKafkaConsumerDbCollection(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	cfg := &config.Config{}
	repo := newMockRetryManager(false)
	fch := make(chan model.Failure)
	dp := newDatabaseProducer(repo, fch, nil)
	hm := HandlerMap{}
	scfg := config.NewSaramaConfig(false, false)
	logger := log.NullLogger{}

	exp := &kafkaConsumerDbCollection{
		cfg:                 cfg,
		producer:            dp,
		retryManager:        repo,
		handler:             newConsumer(fch, cfg, hm, logger),
		handlerMap:          hm,
		saramaCfg:           scfg,
		logger:              logger,
		connectToKafka:      defaultKafkaConnector,
		maintenanceInterval: defaultMaintenanceInterval,
	}

	got := newKafkaConsumerDbCollection(cfg, dp, repo, fch, hm, scfg, logger, defaultKafkaConnector)

	if diff := deep.Equal(exp, got); diff != nil {
		t.Error(diff)
	}
}

func TestKafkaConsumerDbCollection_Start(t *testing.T) {
	defaultDbRetryPollInterval := dbRetryPollInterval
	dbRetryPollInterval = time.Millisecond * 25

	defer func() {
		dbRetryPollInterval = defaultDbRetryPollInterval
	}()

	exampleMsg := &sarama.ConsumerMessage{
		Topic: "product",
		Value: []byte(`{"foo":"bar"}`),
	}

	t.Run("errors when there are no main topics", func(t *testing.T) {
		col := kafkaConsumerDbCollection{cfg: &config.Config{}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.start(ctx, &wg); err == nil {
			t.Error("expected an error but got nil")
		}
		wg.Wait()
	})

	t.Run("errors when it cannot connect to kafka", func(t *testing.T) {
		col, _ := testKafkaConsumerDbCollection(saramatest.NewMockConsumerGroup(), nil, true)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.start(ctx, &wg); err == nil {
			t.Error("expected an error but got nil")
		}
		wg.Wait()
	})

	t.Run("handles errors on kafka consume", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.ErrorOnConsume()
		col, _ := testKafkaConsumerDbCollection(mcg, nil, false)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()
		var wg sync.WaitGroup
		// errors from consume should be logged, but not returned
		if err := col.start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if !mcg.Consumed() {
			t.Error("expected something to have been consumed, but was not")
		}
	})

	t.Run("successful messages are not retried", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		col, repo := testKafkaConsumerDbCollection(mcg, func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return nil
		}, false)

		col.setMaintenanceInterval(time.Millisecond * 20)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if got := repo.getPublishedFailureCountByTopic("product"); got != 0 {
			t.Errorf("expected 0 failures to be produced in database, but got %d", got)
		}

		if got := repo.runMaintenanceCallCount; got != 2 {
			t.Errorf("expected 2 calls to manager.RunMaintenance(), but got %d instead", got)
		}
	})

	t.Run("retries are marked successful when they eventually succeed", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		var called bool
		col, repo := testKafkaConsumerDbCollection(mcg, func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			if !called {
				called = true
				return errors.New("something bad happened")
			}
			return nil
		}, false)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if got := repo.getPublishedFailureCountByTopic("product"); got != 1 {
			t.Errorf("expected 1 failure to be produced in database, but got %d", got)
		}

		if !repo.retrySuccessful {
			t.Error("expected the DB retry to have been marked as successful, but it wasn't")
		}
	})

	t.Run("retries are marked as errored when they continue to fail", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		col, repo := testKafkaConsumerDbCollection(mcg, func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return errors.New("something bad happened")
		}, false)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if got := repo.getPublishedFailureCountByTopic("product"); got != 1 {
			t.Errorf("expected 1 failure to be produced in database, but got %d", got)
		}

		if !repo.retryErrored {
			t.Error("expected the DB retry to have been marked as successful, but it wasn't")
		}
	})

	t.Run("handles error from repository when fetching messages for retry", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		col, repo := testKafkaConsumerDbCollection(mcg, func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return errors.New("something bad happened")
		}, false)
		repo.willErrorOnGetBatch = true

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if got := repo.getPublishedFailureCountByTopic("product"); got != 1 {
			t.Errorf("expected 1 failures to be produced in database, but got %d", got)
		}

		if repo.retryErrored || repo.retrySuccessful {
			t.Error("looks like a retry was updated in the DB, but we did not expect it")
		}
	})

	t.Run("handles error from repository when publishing failure", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		col, repo := testKafkaConsumerDbCollection(mcg, func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return errors.New("something bad happened")
		}, false)
		repo.willErrorOnPublishFailure = true

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if repo.retryErrored || repo.retrySuccessful {
			t.Error("looks like a retry was updated in the DB, but we did not expect it")
		}
	})

	t.Run("gracefully handles messages when there is no registered handler in the handler map", func(t *testing.T) {
		mcg := saramatest.NewMockConsumerGroup()
		mcg.AddMessage(exampleMsg)
		col, repo := testKafkaConsumerDbCollection(mcg, func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return errors.New("something bad happened")
		}, false)

		delete(col.handlerMap, "product")

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		var wg sync.WaitGroup
		if err := col.start(ctx, &wg); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		wg.Wait()

		if repo.retryErrored || repo.retrySuccessful {
			t.Error("looks like a retry was updated in the DB, but we did not expect it")
		}
	})
}

func TestKafkaConsumerDbCollection_Close(t *testing.T) {
	t.Run("consumers are closed", func(t *testing.T) {
		t.Parallel()
		mcg1 := saramatest.NewMockConsumerGroup()
		col := &kafkaConsumerDbCollection{mainKafkaConsumer: mcg1}
		col.close()

		if !mcg1.WasClosed() {
			t.Error("consumer collection was not closed properly")
		}
	})

	t.Run("gracefully handles errors from closes", func(t *testing.T) {
		t.Parallel()
		mcg1 := saramatest.NewMockConsumerGroup()
		mcg1.ErrorOnClose()

		col := &kafkaConsumerDbCollection{mainKafkaConsumer: mcg1, logger: log.NullLogger{}}
		col.close()

		if col.mainKafkaConsumer != nil {
			t.Errorf("consumer collection was not closed properly")
		}
	})

	t.Run("gracefully closes with no main kafka consumer", func(t *testing.T) {
		t.Parallel()

		col := &kafkaConsumerDbCollection{logger: log.NullLogger{}}
		col.close()

		if col.mainKafkaConsumer != nil {
			t.Errorf("consumer collection was not closed properly")
		}
	})
}

func testKafkaConsumerDbCollection(mcg *saramatest.MockConsumerGroup, msgHandler Handler, errorOnConnect bool) (*kafkaConsumerDbCollection, *mockRetryManager) {
	fch := make(chan model.Failure, 10)
	repo := newMockRetryManager(false)
	dp := newDatabaseProducer(repo, fch, log.NullLogger{})

	hm := HandlerMap{"product": msgHandler}
	connector := testKafkaConnector{consumerGroup: mcg, willError: errorOnConnect}

	return newKafkaConsumerDbCollection(newTestConfig(), dp, repo, fch, hm, sarama.NewConfig(), log.NullLogger{}, connector.connectToKafka), repo
}
