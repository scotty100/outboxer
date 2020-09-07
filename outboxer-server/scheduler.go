package outboxer_server

import (
	"fmt"
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
	scheduleC chan bool
}

func NewScheduler(repo outboxer.OutboxRepo, publisher *Publisher, scheduleC chan bool) *Scheduler {

	pC := make(chan []string)
	poller := NewPoller(
		repo,
		publisher,
		pC,
	)
	
	go poller.Run()
	
	return &Scheduler{
		ticker:    time.NewTicker(time.Second *3), // make configurable
		poll:      pC,
		scheduleC: scheduleC,
	}
}

func (s *Scheduler) Run() error {
	for {
		select {
		case <-s.ticker.C:
			fmt.Println(fmt.Printf("Ticker fired ... %s", time.Now().String()))
			s.poll <- PublishableStatuses
		//case <- quit:
		//	s.ticker.Stop()
		//	return
		case <-s.scheduleC:
			fmt.Println(fmt.Printf("Schedule fired ... %s", time.Now().String()))
			s.poll <- PublishableStatuses
		default:
			//fmt.Println(fmt.Printf("Already scheduling...%s", time.Now().String()))
			time.Sleep(5000 * time.Millisecond)
		}
	}

	return nil
}
