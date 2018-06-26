package scheduler

import "db.transfer/core"

type QueuedScheduler struct {
	dataChan     chan *core.InsertDatas
	insertChan   chan chan *core.InsertDatas
	limitChan    chan struct{}
	limitNumber  int //同时进行的进程数量
	InsertNumber int //每个进程进行插入的数量

}

func (s *QueuedScheduler) WorkerChan() chan *core.InsertDatas {
	return make(chan *core.InsertDatas)
}

func (s *QueuedScheduler) Submit(r *core.InsertDatas) {
	s.dataChan <- r
}

func (s *QueuedScheduler) WorkerReady(w chan *core.InsertDatas) {
	s.insertChan <- w
}

func (s *QueuedScheduler) WorkerLimit(limit int, insertNumebr int) {
	s.InsertNumber = insertNumebr
	s.limitNumber = limit
	s.limitChan = make(chan struct{}, limit)

}

func (s *QueuedScheduler) Run() {

	s.insertChan = make(chan chan *core.InsertDatas)
	s.dataChan = make(chan *core.InsertDatas)

	go func() {
		dataQ := make(map[string]*core.InsertDatas)
		var workerQ []chan *core.InsertDatas
		for {

			var activeInsert *core.InsertDatas

			var activeWorker chan *core.InsertDatas

			if len(dataQ) > 0 && len(workerQ) > 0 && len(s.limitChan) < s.limitNumber+1 {

				for _, tableInfo := range dataQ {
					if len(*tableInfo.Data) == 0 {
						continue
					}
					if len(*tableInfo.Data) > s.InsertNumber {
						insertData := (*tableInfo.Data)[:s.InsertNumber]
						activeInsert = &core.InsertDatas{
							&insertData,
							tableInfo.DbInfo,
						}
						break
					}
					if len(*tableInfo.Data) < s.InsertNumber {
						insertData := (*tableInfo.Data)[:]
						activeInsert = &core.InsertDatas{
							&insertData,
							tableInfo.DbInfo,
						}
						break
					}
				}
				activeWorker = workerQ[0]
			}
			select {
			case r := <-s.dataChan:
				dbInfo, ok := dataQ[r.DbInfo.Table]
				if !ok {
					dataQ[r.DbInfo.Table] = r
				} else {
					*dbInfo.Data = append(*dbInfo.Data, *r.Data...)
				}
			case w := <-s.insertChan:
				workerQ = append(workerQ, w)

			case activeWorker <- activeInsert:

				tableInfo := dataQ[activeInsert.DbInfo.Table]

				if len(*tableInfo.Data) > s.InsertNumber {
					temp := (*tableInfo.Data)[:s.InsertNumber]
					*tableInfo.Data = temp

				}
				if len(*tableInfo.Data) < s.InsertNumber {
					temp := (*tableInfo.Data)[:]
					*tableInfo.Data = temp

				}
				workerQ = workerQ[1:]
				s.limitChan <- struct{}{}
			}
		}
	}()
}
