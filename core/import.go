package core

import (
	"bytes"
	"math"
	"sync"
	"time"

	"sync/atomic"

	"github.com/cihub/seelog"
)

//开始进行插入预处理，判断开启的进程个数
func (d *Deliver) ImportDispatch() {

	for _, importTable := range d.Import {

		GoroutineImportNumber := int(importTable.GoroutineNumber)

		goNumber := math.Ceil(float64(len(importTable.InsertData)) / importTable.GoroutineNumber)

		seelog.Infof("数据表%s开始进行导入", importTable.Table)
		seelog.Infof("数据表%s开启进程数量为：%v", importTable.Table, goNumber)
		seelog.Infof("数据表%s要插入的记录数量为：%v", importTable.Table, len(importTable.InsertData))
		seelog.Infof("数据表%s不完善的记录数量为：%v", importTable.Table, importTable.NoInsertCount)

		beginTime := time.Now().Unix()

		var wg sync.WaitGroup

		for i := 0; i < int(goNumber); i++ {

			start := i * GoroutineImportNumber

			end := (i + 1) * GoroutineImportNumber

			var tempSlice []map[string]string

			if i == int(goNumber)-1 {

				tempSlice = importTable.InsertData[start:]
			} else {
				tempSlice = importTable.InsertData[start:end]
			}

			wg.Add(1)

			importTable.Ch <- struct{}{}

			go func(i int) {

				seelog.Infof("表%s的进程%v开启", importTable.Table, i+1)

				if !d.Predict {
					d.Insert(importTable, tempSlice, start, end)
				}

				defer wg.Done()
			}(i)

		}

		wg.Wait()
		finishTime := time.Now().Unix()
		seelog.Infof("表%s实际消耗时间为：%v秒", importTable.Table, finishTime-beginTime)
	}

}

func (d *Deliver) Insert(insertDatas InsertDatas) {

	importTable := insertDatas.DbInfo
	transfersSlice := insertDatas.Data
	start := insertDatas.StartId
	end := insertDatas.EndId

	var buffer bytes.Buffer

	buffer.WriteString("INSERT INTO " + importTable.Table + " (")

	for _, field := range importTable.Fields {

		buffer.WriteString(" " + field + ",")

	}
	buffer.Truncate(buffer.Len() - 1)

	buffer.WriteString(")VALUES ")

	for _, item := range transfersSlice {
		buffer.WriteString("(")
		for _, field := range importTable.Fields {
			buffer.WriteString(item[field])
			buffer.WriteString(",")
		}
		buffer.Truncate(buffer.Len() - 1)

		buffer.WriteString("),")
	}
	buffer.Truncate(buffer.Len() - 1)

	if d.Test.Open {
		seelog.Info(buffer.String())
	}

	rs, err := importTable.Db.Exec(buffer.String())

	if err != nil {
		seelog.Errorf("表%s插入数据出错，出错索引为%v-%v,出错原因%v", importTable.Table, start, end, err)
	}

	lines, err := rs.RowsAffected()

	if err != nil {
		seelog.Errorf("表%s获取插入行数出错%v", importTable.Table, err)
	}
	atomic.AddInt64(&importTable.ActualInsertCount, lines)

	seelog.Infof("表%s插入的行数为%v", importTable.Table, lines)

	<-importTable.Ch
}
