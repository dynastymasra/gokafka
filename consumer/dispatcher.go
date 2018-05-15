package main

type Dispatcher struct {
	WorkerPool chan chan Job
	MaxWorkers int
	JobQueue   chan Job
}

func NewDispatcher(maxWorker int, jobQueue chan Job) *Dispatcher {
	pool := make(chan chan Job, maxWorker)
	return &Dispatcher{
		WorkerPool: pool,
		MaxWorkers: maxWorker,
		JobQueue:   jobQueue,
	}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.WorkerPool, i)
		go worker.Start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.JobQueue:
			go func(job Job) {
				jobChannel := <-d.WorkerPool
				jobChannel <- job
			}(job)
		}
	}
}
