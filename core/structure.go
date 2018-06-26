package core

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/cihub/seelog"
)

type Scheduler interface {
	Submit(InsertDatas)
	WorkerChan() chan InsertDatas
	Run()
	WorkerReady(chan InsertDatas)
}

type InsertDatas struct {
	Data    []map[string]string
	DbInfo  *DbInfo
	StartId int64
	EndId   int64
}

type DbInfo struct {
	Db                *sql.DB
	Table             string
	Fields            []string
	FieldsValue       map[string]interface{}
	GoroutineNumber   float64             //每个goroutine操作数据的数量
	Ch                chan struct{}       //管理同时进行的进程数量
	InsertData        []map[string]string //将要进行插入的数据
	NoInsertCount     int64               //不进行插入的数据数量统计
	ActualInsertCount int64               //实际插入数量统计
}

type Attach struct {
	Data map[string]*sync.Map     //附加的数据，如插入时依赖某个表
	Chs  map[string]chan struct{} //附加的数据是否完成
}

type Test struct {
	Open      bool
	HasOutput int32 //调试语句是否已经输出,0未输出
	Number    int64 // 调试的数量
}

type Deliver struct {
	Import map[string]*DbInfo
	Export DbInfo
	Attach Attach

	Scheduler Scheduler //通过队列实现一边读取，一边插入
	sync.Mutex
	ProcessNumber int64
	Test          Test //进行100行的数据插入测试
	Predict       bool //预处理，不进行实际插入

}

type ValueFunc func(map[string]*[]byte) string

//赋值给新表字段时的处理
func Value(field string, defaultValue string) ValueFunc {

	return func(row map[string]*[]byte) string {

		fieldVal, ok := row[field]

		if field != "" && !ok {
			seelog.Infof("该字段未读取 %s", field)
			seelog.Flush()
			os.Exit(0)
		}
		if !ok {
			return fmt.Sprintf("%q", defaultValue)
		}

		return fmt.Sprintf("%q", *fieldVal)
	}

}
