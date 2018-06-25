package core

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/cihub/seelog"
)

type DbInfo struct {
	Db              *sql.DB
	Table           string
	Field           []string
	FieldValue      map[string]interface{}
	GoroutineNumber float64       //每个goroutine操作数据的数量
	Ch              chan struct{} //管理同时进行的进程数量
}

type Attach struct {
	Data map[string]*sync.Map     //附加的数据，如插入时依赖某个表
	Chs  map[string]chan struct{} //附加的数据是否完成
}

type Deliver struct {
	Import DbInfo
	Export DbInfo
	Attach Attach

	sync.Mutex

	Test    bool //进行100行的数据插入测试
	Predict bool //预处理，不进行实际插入

	InsertData []map[string]string //将要进行插入的数据

	NoInsertCount int64 //不进行插入的数据数量统计

	ActualInsertCount int64 //实际插入数量统计
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
