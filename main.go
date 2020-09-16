package main

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"io.benefexapps/outboxer/outboxer_prometheus"
	"time"

	"github.com/BenefexLtd/infrastructure/messaging"
	"github.com/BenefexLtd/onehub-go-base/pkg/logging"
	"github.com/BenefexLtd/onehub-go-base/pkg/mongo"
	"io.benefexapps/outboxer/outboxer"
	client "io.benefexapps/outboxer/outboxer-client"
	server "io.benefexapps/outboxer/outboxer-server"

	uuid "github.com/google/uuid"
)

func main() {
	fmt.Println("Starting the application")

	reg := prometheus.NewRegistry()
	metAdapter := outboxer_prometheus.NewPrometheusMetricsAdapter(reg)
	logger := logging.NewLogger("INFO", "outboxer-server", "1.0")
	mongo := mongo.New("mongodb://localhost:27017/comments_default", 15, "comments_default", logger)
	outboxRepo := &outboxer.MongoOutboxRepo{
		Store:        mongo,
		QueryMaxTime: 10,
	}
	seqRepo := &outboxer.MongoSeqRepo{
		Store: mongo,
	}

	ctx := context.Background()

	client := client.NewOutboxClient(outboxRepo, seqRepo, metAdapter)
	addSomeOutboxMessages(ctx, logger, client)

	pubSubClient, _ := server.GetClient(ctx, "benefex-onehub-dev")
	publisher, _ := server.NewPublisher(ctx, pubSubClient, []string{"scott-test"}, metAdapter)

	scheduleC := make(chan bool)
	doneC := make(chan bool)
	scheduler := server.NewScheduler(outboxRepo, logger, metAdapter, publisher, scheduleC, doneC)

	defer close(scheduleC)
	defer close(doneC)

	errc := make(chan error, 1)
	go func() {
		errc <- scheduler.Run()
	}()

	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(time.Second * 1)
			scheduleC <- true
		}
	}()

	go func() {

		time.Sleep(time.Second * 10)
		doneC <- true

	}()

	for {
		if err := <-errc; err != nil {
			fmt.Printf(" error occurred: %s", err)
		}
	}

}

// TestMessage for testing
type TestMessage struct {
	ID string `json:"_id" validate:"required"`
}

func addSomeOutboxMessages(ctx context.Context, logger logging.Logger, oc *client.OutboxClient) {

	companyID := "benefex"
	aggregateType := "test"
	topic := "scott-test"
	messageType := "test-message"
	event := messaging.OneHubEvent{Content: &TestMessage{ID: uuid.New().String()}}
	createdBy := uuid.New().String()

	if _, e := oc.AddOutboxMessage(ctx, companyID, aggregateType, uuid.New().String(), topic, messageType, createdBy, event, time.Now()); e != nil {
		logger.Errorf("Error creating outbox record: %s", e.Error())
	}
	if _, e := oc.AddOutboxMessage(ctx, companyID, aggregateType, uuid.New().String(), topic, messageType, createdBy, event, time.Now()); e != nil {
		logger.Errorf("Error creating outbox record: %s", e.Error())
	}
	if _, e := oc.AddOutboxMessage(ctx, companyID, aggregateType, uuid.New().String(), topic, messageType, createdBy, event, time.Now()); e != nil {
		logger.Errorf("Error creating outbox record: %s", e.Error())
	}
}
