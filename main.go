package main

import (
	"context"
	"fmt"
	"github.com/BenefexLtd/infrastructure/messaging"
	"github.com/BenefexLtd/onehub-go-base/pkg/logging"
	"github.com/BenefexLtd/onehub-go-base/pkg/mongo"
	"io.benefexapps/outboxer/outboxer"
	client "io.benefexapps/outboxer/outboxer-client"
	server "io.benefexapps/outboxer/outboxer-server"
	"time"

	uuid "github.com/google/uuid"
)

func main() {
	fmt.Println("Starting the application")

	logger := logging.NewLogger("INFO", "outboxer-server", "1.0")
	mongo := mongo.New("mongodb://localhost:27017/comments_default", 15, "comments_default", logger)
	outboxRepo := &outboxer.MongoOutboxRepo{
		Store:        mongo,
		QueryMaxTime: 10,
	}
	seqRepo := &outboxer.MongoSeqRepo{
		Store:        mongo,
		QueryMaxTime: 10,
	}

	ctx := context.Background()

	client := client.NewOutboxClient(outboxRepo, seqRepo)
	addSomeOutboxMessages(ctx, client)

	pubSubClient, _ := server.GetClient(ctx, "benefex-onehub-dev")
	publisher, _ := server.NewPublisher(ctx, pubSubClient, []string{"scott-test"})

	scheduleC :=  make(chan bool)
	scheduler := server.NewScheduler(outboxRepo, publisher, scheduleC)

	errc := make(chan error, 1)
	go func() {

		errc <- scheduler.Run()
	}()


	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(time.Second *1)
			scheduleC <- true
		}
		close(scheduleC)
	}()

	for {
		if err := <-errc; err != nil {
			fmt.Printf(" error occurred: %s", err)
		}
	}

}

type TestMessage struct {
	Id string `json:"_id", validate:"required"`
}

func addSomeOutboxMessages(ctx context.Context, oc *client.OutboxClient) {

	companyId := "benefex"
	aggregateType := "test"
	topic := "scott-test"
	messageType := "test-message"
	event := messaging.OneHubEvent{Content: &TestMessage{Id: uuid.New().String()}}
	createdBy := uuid.New().String()

	oc.AddOutboxMessage(ctx, companyId, aggregateType, uuid.New().String(), topic, messageType, createdBy, event, time.Now())
	oc.AddOutboxMessage(ctx, companyId, aggregateType, uuid.New().String(), topic, messageType, createdBy, event, time.Now())
	oc.AddOutboxMessage(ctx, companyId, aggregateType, uuid.New().String(), topic, messageType, createdBy, event, time.Now())
}
