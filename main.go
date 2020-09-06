package main

import (
	"context"
	"fmt"
	"github.com/BenefexLtd/onehub-go-base/pkg/logging"
	"github.com/BenefexLtd/onehub-go-base/pkg/mongo"
	"io.benefexapps/outboxer/outboxer"
	outboxer2 "io.benefexapps/outboxer/outboxer-server"
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

	ctx := context.Background()
	pubSubClient, _ := outboxer2.GetClient(ctx, "benefex-onehub-dev")
	publisher, _ := outboxer2.NewPublisher(ctx, pubSubClient, "scott-test")

	worker := outboxer2.NewWorker(outboxRepo, publisher)

	addSomeOutboxMessages(outboxRepo)

	//worker.Start()

	go func() {
		for e := range worker.Errors() {

			fmt.Println("error from worker", e.Err)
			worker.Stop()
		}
	}()

	for {
		worker.Start()
	}

}

func addSomeOutboxMessages(or *outboxer.MongoOutboxRepo) {

	messages := make([]*outboxer.Outbox, 3)
	messages[0] = &outboxer.Outbox{
		Id:                 uuid.New().String(),
		AggregateId:        uuid.New().String(),
		Version:            "1",
		Topic:              "scott-test",
		MessageType:        "test-mesage",
		Payload:            "{\"content\":{\"test-prop\":\"test-val\"}}",
		State:              0,
		CreatedDateTime:    time.Now().Add(time.Duration(-10) * time.Second ),
		ProcessingDateTime: time.Time{},
		SentDateTime:       time.Time{},
		ExternalMessageId:  "",
		WorkerId:           "",
	}
	messages[1] = &outboxer.Outbox{
		Id:                 uuid.New().String(),
		AggregateId:        uuid.New().String(),
		Version:            "1",
		Topic:              "scott-test",
		MessageType:        "test-mesage",
		Payload:            "{\"content\":{\"test-prop1\":\"test-val1\"}}",
		State:              0,
		CreatedDateTime:    time.Now().Add(time.Duration(-10) * time.Second ),
		ProcessingDateTime: time.Time{},
		SentDateTime:       time.Time{},
		ExternalMessageId:  "",
		WorkerId:           "",
	}
	messages[2] = &outboxer.Outbox{
		Id:                 uuid.New().String(),
		AggregateId:        uuid.New().String(),
		Version:            "1",
		Topic:              "scott-test",
		MessageType:        "test-mesage",
		Payload:            "{\"content\":{\"test-prop2\":\"test-val2\"}}",
		State:              0,
		CreatedDateTime:    time.Now().Add(time.Duration(-10) * time.Second ),
		ProcessingDateTime: time.Time{},
		SentDateTime:       time.Time{},
		ExternalMessageId:  "",
		WorkerId:           "",
	}

	for _, m := range messages {
		or.Add(context.Background(), m)
	}
}
