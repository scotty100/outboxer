package outboxer_server

import (
	"io.benefexapps/outboxer/outboxer"
	"time"
)

// user ticker to schedule the polling on the poller struct
// use select with a default clause so that if the poller cannot receive the message as it is already running then the default clause will hit
// we only want one instance of the poller processing items at a time
// https://gobyexample.com/non-blocking-channel-operations

// also provide ability for client to call to schedule immediately and call for an immediate poll

var PublishableStatuses = []string {outboxer.Created, outboxer.Error_Retry}

type Scheduler struct {
	ticker    *time.Ticker // periodic ticker
	poll      chan []string
}

func NewScheduler(repo outboxer.OutboxRepo, publisher Publisher) *Scheduler {

	pC := make(chan []string)
	poller := NewPoller(
		repo,
		publisher,
		pC,
	)
	
	go poller.Run()
	
	return &Scheduler{
		ticker:    time.NewTicker(time.Millisecond * 3000), // make configurable
		poll:      pC,
	}
}

func (s *Scheduler) run(){
	for {
		select {
		case <-s.ticker.C:
			s.poll <- PublishableStatuses
		}
	}
}
