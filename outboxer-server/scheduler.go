package server

import (
	"fmt"
	"time"

	"github.com/BenefexLtd/onehub-go-base/pkg/logging"
	"io.benefexapps/outboxer/outboxer"
)

// user ticker to schedule the polling on the poller struct
// use select with a default clause so that if the poller cannot receive the message as it is already running then the default clause will hit
// we only want one instance of the poller processing items at a time
// https://gobyexample.com/non-blocking-channel-operations

// also provide ability for client to call to schedule immediately and call for an immediate poll

// PublishableStatuses set of statuses to include in publishing
var PublishableStatuses = []string{outboxer.Created, outboxer.Error_Retry}

// Scheduler is struct to control the scheduling of outbox polling. Includes periodic poll as well as the ability to schedule a poll on command
type Scheduler struct {
	ticker    *time.Ticker // periodic ticker
	poll      chan PollRequest
	pollDone  chan bool
	scheduleC chan bool
	doneC     chan bool
}

// NewScheduler - create a new Scheduler
func NewScheduler(repo outboxer.OutboxRepo, logger logging.Logger, metrics outboxer.Metrics, publisher Publisher, scheduleC chan bool, doneC chan bool) Scheduler {

	pC := make(chan PollRequest)
	pollerDoneC := make(chan bool)
	poller := NewPoller(
		repo,
		logger,
		metrics,
		publisher,
		pC,
		pollerDoneC,
	)

	go poller.Run()

	return Scheduler{
		ticker:    time.NewTicker(time.Second * 3), // make configurable
		poll:      pC,
		scheduleC: scheduleC,
		pollDone:  pollerDoneC,
		doneC:     doneC,
	}
}

// Run the scheduler
func (s *Scheduler) Run() error {
	defer close(s.poll)
	defer close(s.pollDone)
	for {
		select {
		case <-s.ticker.C:
			fmt.Println(fmt.Printf("Ticker fired ... %s", time.Now().String()))
			s.poll <- PollRequest{
				statuses: PublishableStatuses,
				client:   "ticker",
			}
		case <-s.scheduleC:
			fmt.Println(fmt.Printf("Schedule fired ... %s", time.Now().String()))
			s.poll <- PollRequest{
				statuses: PublishableStatuses,
				client:   "schedule",
			}
		case <-s.doneC:
			fmt.Println(fmt.Printf("Done fired ... %s", time.Now().String()))
			s.pollDone <- true
			return nil
		default:
			//fmt.Println(fmt.Printf("Already scheduling...%s", time.Now().String()))
			fmt.Println(fmt.Printf("Default fired ... %s", time.Now().String()))
			time.Sleep(1000 * time.Millisecond)
		}
	}
}
