package factotum

import (
	"sync"
)

type WorkRequest interface {
	Work() bool
}

type worker struct {
	ID          int
	work        chan WorkRequest
	workerQueue chan chan WorkRequest
	quit        chan bool
	wait        *sync.WaitGroup
}

type WorkerGroup struct {
	workerQueue chan chan WorkRequest
	WorkQueue   chan WorkRequest
	workers     []*worker
	wait        *sync.WaitGroup
	closed      bool
	quit        chan bool
}

func NewWorker(id int, wq chan chan WorkRequest, wait *sync.WaitGroup) *worker {
	worker := &worker{
		ID:          id,
		work:        make(chan WorkRequest),
		workerQueue: wq,
		quit:        make(chan bool),
		wait:        wait,
	}

	return worker
}

func (w *worker) Start() {
	go func() {
		defer w.wait.Done()
		for {
			w.workerQueue <- w.work

			select {
			case work := <-w.work:
				work.Work()
			case <-w.quit:
				return
			}
		}
	}()
}

func (w *worker) Stop() {
	go func() {
		close(w.quit)
	}()
}

func NewWorkerGroup(n int) (wg *WorkerGroup) {
	wg = &WorkerGroup{}
	wg.workerQueue = make(chan chan WorkRequest, n)
	wg.WorkQueue = make(chan WorkRequest, n*2)
	wg.workers = make([]*worker, n)
	wg.quit = make(chan bool)
	wg.wait = &sync.WaitGroup{}
	wg.wait.Add(n)
	for i := 0; i < n; i++ {
		w := NewWorker(i+1, wg.workerQueue, wg.wait)
		wg.workers[i] = w
		w.Start()
	}
	return wg
}

func (wg *WorkerGroup) Start() {
	go func() {
		for {
			select {
			case work := <-wg.WorkQueue:
				go func() {
					worker := <-wg.workerQueue

					worker <- work
				}()
			case <-wg.quit:
				return
			}
		}
	}()
}

func (wg *WorkerGroup) Enqueue(w WorkRequest) bool {
	if wg.closed {
		return false
	}
	go func() {
		wg.WorkQueue <- w
	}()
	return true
}

func (wg *WorkerGroup) Stop() {
	if wg.closed {
		return
	}
	close(wg.quit)
	wg.closed = true
	for _, w := range wg.workers {
		w.Stop()
	}
	wg.wait.Wait()
	close(wg.WorkQueue)
	close(wg.workerQueue)
}
