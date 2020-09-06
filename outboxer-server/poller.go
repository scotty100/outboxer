package outboxer_server

import "io.benefexapps/outboxer/outboxer"

// poller will recieve a call to poll for messages on channel with statues to check for re-processing
// the poller will loop whilst there are messages to process publishing one at a time
// once no messages are ready to process it will call back on done channel

type Poller struct {

	repo outboxer.OutboxRepo
	poll chan []string
}

func NewPoller (repo outboxer.OutboxRepo, poll chan []string, done chan<- struct{}) *Poller {
	return &Poller{
		repo: repo,
		poll: poll,
	}
}

func  (p *Poller) Run() {
	for {
		select {
		case <-p.poll:
			// run the polling process
		}
	}
}

func (p *Poller) processItems() {


	// loop whilst outboxer to process
		// for each
			// try to publish and update the message accordingly
}
