//package outboxer_server
//
//import (
//	"context"
//	"fmt"
//	"io.benefexapps/outboxer/outboxer"
//	"runtime"
//	"sync"
//	"time"
//)
//
//// Possible worker states.
//const (
//	Stopped = 0
//	Paused  = 1
//	Running = 2
//)
//
//type OutboxerError struct {
//	Err    error
//	Ctx    context.Context
//	Outbox *outboxer.Outbox
//}
//
//type OutboxerEvent struct {
//	Ctx    context.Context
//	Outbox *outboxer.Outbox
//}
//
//type Worker struct {
//	outRep outboxer.OutboxRepo
//	wg     sync.WaitGroup
//	state  chan int
//	errCh  chan OutboxerError
//	workCh chan OutboxerEvent
//}
//
//func NewWorker(outRepo outboxer.OutboxRepo, publisher *Publisher) *Worker {
//	w := new(Worker)
//
//	w.outRep = outRepo
//	//w.publisher = publisher
//	w.state = make(chan int, 1)
//	w.errCh = make(chan OutboxerError, 100)
//	w.workCh = make(chan OutboxerEvent, 10000)
//
//	go w.publish(publisher, outRepo, w.workCh)
//
//	return w
//}
//
//func (w *Worker) setState(state int) {
//	w.state <- state
//}
//
//func (w *Worker) Start() {
//	go w.run()
//
//	go func() {
//		w.setState(Running)
//	}()
//}
//
//func (w *Worker) Stop() {
//	go func() {
//		w.setState(Stopped)
//	}()
//}
//
//func (b *Worker) Errors() <-chan OutboxerError {
//	return b.errCh
//}
//
////func (w *Worker) Process() error{
////	for {
////		run()
////	}
////	return nil
////}
//
//func (w *Worker) run() error {
//	w.wg.Add(1)
//	defer w.wg.Done()
//
//	ctx := context.Background()
//	//var docOffset projectorOffset
//	//docOffset.new(w.p.Name(), w.p.Bit())
//	//filter := docOffset.filterQuery(w.p.Bit())
//	//updateInprog := docOffset.updateInprog(w.p.Bit())
//	//updateDone := docOffset.updateDone(w.p.Bit())
//	//changeInprog := mgo.Change{Update: updateInprog, ReturnNew: true}
//	//changeDone := mgo.Change{Update: updateDone, ReturnNew: true}
//	state := Paused
//
//	for {
//		select {
//		case state = <-w.state:
//			switch state {
//			case Running:
//				for {
//
//					// get next record
//					o, err := w.outRep.FindNextOutboxesForProcessing(ctx)
//					if err != nil {
//						w.errCh <- OutboxerError{Err: fmt.Errorf("error getting next event to publish : %s", err.Error()), Ctx: ctx, Outbox: nil}
//					}
//
//					if o != nil {
//						oe := OutboxerEvent{Ctx: ctx, Outbox: o}
//						w.workCh <- oe
//					} else {
//						// sleep for config ...default to a second a the moment
//
//						fmt.Println("no messages so sleeping....")
//						time.Sleep(time.Second * time.Duration(5))
//					}
//
//					go w.setState(Paused)
//					break
//				}
//			}//case Paused:
//
//		default:
//			runtime.Gosched()
//		}
//		//time.Sleep(time.Second)
//	}
//	return nil
//}
//
//func (w *Worker) publish(publisher *Publisher, or outboxer.OutboxRepo, ch <-chan OutboxerEvent) {
//	w.wg.Add(1)
//	defer w.wg.Done()
//
//
//
//	for o := range ch {
//
//		fmt.Printf("publishing event : %s \r",o.Outbox.Id )
//
//		id, err := publisher.Publish(o.Ctx, o)
//		if err != nil {
//
//			or.SetMessageState(o.Ctx, o.Outbox.Id, 2, time.Now(), "")
//
//			// puiblish the error occurred
//			w.errCh <- OutboxerError{Err: fmt.Errorf("could not publish event (%s): %s", o.Outbox.Id, err.Error()), Ctx: o.Ctx, Outbox: o.Outbox}
//		}
//
//		or.SetMessageState(o.Ctx, o.Outbox.Id, 3, time.Now(), id)
//	}
//}
//
//// add timout validator to run across outbox
//
//// add error checker ... see if we can retry publishing a message
