package server

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/BenefexLtd/onehub-go-base/pkg/logging"
	"golang.org/x/net/context"
	"io.benefexapps/outboxer/outboxer"
)

// poller will receive a call to poll for messages on channel with statues to check for re-processing
// the poller will loop whilst there are messages to process publishing one at a time
// once no messages are ready to process it will call back on done channel

// MaxRetries maximum number of times an outbox record is attempted to be published
const MaxRetries = 5

// PollRequest ... state object passed on poll requests
type PollRequest struct {
	statuses []string
	client   string
}

// Poller struct to handle polling of outbox resource for messages to publish
type Poller struct {
	repo      outboxer.OutboxRepo
	logger    logging.Logger
	metrics   outboxer.Metrics
	publisher Publisher
	poll      chan PollRequest
	done      chan bool
	closing   bool
}

// NewPoller create a new Poller
func NewPoller(repo outboxer.OutboxRepo, logger logging.Logger, metrics outboxer.Metrics, publisher Publisher, poll chan PollRequest, done chan bool) Poller {
	return Poller{
		repo:      repo,
		logger:    logger,
		metrics:   metrics,
		publisher: publisher,
		poll:      poll,
		done:      done,
		closing:   false,
	}
}

// Run run the poller
func (p Poller) Run() {

	for {
		select {
		case req := <-p.poll:
			fmt.Println(fmt.Printf("Polling for client %s ... %s", req.client, time.Now().String()))
			p.processItems(req.statuses)
		case <-p.done:
			p.closing = true
			return
		}
	}
}

func (p Poller) processItems(statuses []string) {

	ctx := context.Background()

	fmt.Println(fmt.Printf("Getting messages ... %s", time.Now().String()))
	o, err := p.repo.GetNextOutbox(ctx, statuses)

	for err == nil && o.ID != 0 {

		if p.closing {
			fmt.Println(fmt.Printf("Poller is closing ... %s", time.Now().String()))
			return
		}

		fmt.Println(fmt.Printf("Publishing message ... %s", time.Now().String()))
		messageID, pubErr := p.publisher.Publish(ctx, o)
		if pubErr != nil {
			o.Retries++
			updatedStatus := outboxer.Error_Retry
			if o.Retries == MaxRetries {
				updatedStatus = outboxer.Error
			}
			if outErr := p.repo.SetMessagePublishFailed(ctx, o.ID, updatedStatus, o.Retries); outErr != nil {
				p.logger.Errorf("error setting message as failed for outbox id: %s", o.ID)
			}
		}

		if e := p.repo.SetMessageProcessed(ctx, o.ID, outboxer.Published, time.Now(), messageID); e != nil {
			p.logger.Errorf("error setting message as processed for outbox id: %s", o.ID)
		}

		o, err = p.repo.GetNextOutbox(ctx, statuses)
		if err != nil && err != mongo.ErrNoDocuments {
			p.logger.Errorf("error getting next outbox message to process", o.ID)
			p.metrics.IncOutboxFind(outboxer.MetricsError)
		}
		p.metrics.IncOutboxFind(outboxer.MetricsSuccess)
	}

	//time.Sleep(1 * time.Second)
	fmt.Println(fmt.Printf("Poller finished ... %s", time.Now().String()))

}
