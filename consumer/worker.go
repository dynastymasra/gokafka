package main

import (
	"reflect"
	"runtime"

	"fmt"

	log "github.com/dynastymasra/gochill"
)

// Worker represents goroutine worker to process received message
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

// NewWorker creates new worker instance
func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
	}
}

// Start method starts the run loop for the worker
func (w Worker) Start() {
	go func() {
		for {
			w.WorkerPool <- w.JobChannel
			select {
			case job := <-w.JobChannel:
				if job.Error != nil {
					log.Error(log.Msg("Received consumer get errors", job.Error.Error()),
						log.O("package", runtime.FuncForPC(reflect.ValueOf(w.Start).Pointer()).Name()),
						log.O("brokers", job.Broker), log.O("topics", job.Topics))
					break
				}

				if job.Notification != nil {
					log.Warn(log.Msg("Received consumer notification", fmt.Sprintf("%+v", job.Notification)),
						log.O("package", runtime.FuncForPC(reflect.ValueOf(w.Start).Pointer()).Name()),
						log.O("brokers", job.Broker), log.O("topics", job.Topics))
					break
				}

				log.Info(log.Msg("Received message from broker", string(job.Payload.Value)), log.O("offset", job.Payload.Offset),
					log.O("package", runtime.FuncForPC(reflect.ValueOf(w.Start).Pointer()).Name()),
					log.O("brokers", job.Broker), log.O("topic", job.Payload.Topic), log.O("partition", job.Payload.Partition))
			case <-w.quit:
				log.Warn(log.Msg("We have received a signal to stop"),
					log.O("package", runtime.FuncForPC(reflect.ValueOf(w.Start).Pointer()).Name()))
				return
			}
		}
	}()
}

func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}
