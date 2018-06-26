package scheduler

import "db.transfer/core"

type QueuedScheduler struct {
	dataChan   chan core.InsertDatas
	insertChan chan chan core.InsertDatas
	limitChan  chan struct{}
}

func (s *QueuedScheduler) WorkerChan() chan core.InsertDatas {
	return make(chan core.InsertDatas)
}

func (s *QueuedScheduler) Submit(r core.InsertDatas) {
	s.dataChan <- r
}

func (s *QueuedScheduler) WorkerReady(w chan core.InsertDatas) {
	s.insertChan <- w
}

func (s *QueuedScheduler) WorkerLimit(limit int64) {
	s.limitChan = make(chan struct{}, limit)
}

func (s *QueuedScheduler) Run() {
	s.insertChan = make(chan chan core.InsertDatas)
	s.dataChan = make(chan core.InsertDatas)

	go func() {
		var requestQ []core.InsertDatas
		var workerQ []chan core.InsertDatas
		for {
			var activeRequest core.InsertDatas
			var activeWorker chan core.InsertDatas
			if len(requestQ) > 0 && len(workerQ) > 0 {
				activeRequest = requestQ[0]
				activeWorker = workerQ[0]
			}
			select {
			case r := <-s.dataChan:
				requestQ = append(requestQ, r)
			case w := <-s.insertChan:
				workerQ = append(workerQ, w)
			case activeWorker <- activeRequest:
				workerQ = workerQ[1:]
				requestQ = requestQ[1:]

			}
		}
	}()
}
