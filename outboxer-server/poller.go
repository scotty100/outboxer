package outboxer_server

import (
	"fmt"
	"golang.org/x/net/context"
	"io.benefexapps/outboxer/outboxer"
	"time"
)

// poller will receive a call to poll for messages on channel with statues to check for re-processing
// the poller will loop whilst there are messages to process publishing one at a time
// once no messages are ready to process it will call back on done channel

// move to config
const MaxRetries = 5

type Poller struct {
	repo      outboxer.OutboxRepo
	publisher *Publisher
	poll      chan []string
}

func NewPoller(repo outboxer.OutboxRepo, publisher *Publisher, poll chan []string) *Poller {
	return &Poller{
		repo:      repo,
		publisher: publisher,
		poll:      poll,
	}
}

func (p *Poller) Run() {
	for {
		select {
		case statuses := <-p.poll:
			p.processItems(statuses)
		}
	}
}

func (p *Poller) processItems(statuses []string) {

	ctx := context.Background()
	// loop whilst outboxer to process
	// for each
	// try to publish and update the message accordingly

	fmt.Println(fmt.Printf("Getting messages ... %s", time.Now().String()))
	o, err := p.repo.GetNextOutbox(ctx, statuses)

	for err == nil && o.Id != 0 {
		fmt.Println(fmt.Printf("Publishing message ... %s", time.Now().String()))
		messageId, pubErr := p.publisher.Publish(ctx, o)
		if pubErr != nil {
			o.Retries++
			updatedStatus := outboxer.Error_Retry
			if o.Retries == MaxRetries {
				updatedStatus = outboxer.Error
			}
			p.repo.SetMessagePublishFailed(ctx, o.Id, updatedStatus, o.Retries)
		}

		p.repo.SetMessageProcessed(ctx, o.Id, outboxer.Published, time.Now(), messageId)

		o, err = p.repo.GetNextOutbox(ctx, statuses)
	}

	time.Sleep(1 * time.Second)

}
