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

	GoroutineImportNumber := int(d.Import.GoroutineNumber)

	goNumber := math.Ceil(float64(len(d.InsertData)) / d.Import.GoroutineNumber)

	seelog.Infof("开启进程数量为：%v", goNumber)
	seelog.Infof("要插入的记录数量为：%v", len(d.InsertData))
	seelog.Infof("不完善的记录数量为：%v", d.NoInsertCount)

	beginTime := time.Now().Unix()

	var wg sync.WaitGroup

	for i := 0; i < int(goNumber); i++ {

		start := i * GoroutineImportNumber

		end := (i + 1) * GoroutineImportNumber

		var tempSlice []map[string]string

		if i == int(goNumber)-1 {

			tempSlice = d.InsertData[start:]
		} else {
			tempSlice = d.InsertData[start:end]
		}

		wg.Add(1)

		d.Import.Ch <- struct{}{}

		go func(i int) {
			//_ = tempSlice
			seelog.Infof("进程%v开启", i+1)

			if !d.Predict {
				d.Insert(tempSlice, start, end)
			}

			defer wg.Done()
		}(i)

	}

	wg.Wait()
	finishTime := time.Now().Unix()
	seelog.Infof("实际消耗时间为：%v秒", finishTime-beginTime)
}

func (d *Deliver) Insert(transfersSlice []map[string]string, start int, end int) {

	var buffer bytes.Buffer

	buffer.WriteString("INSERT INTO " + d.Import.Table + " (")

	for _, field := range d.Import.Fields {
		buffer.WriteString(" " + field + ",")
	}
	buffer.Truncate(buffer.Len() - 1)

	buffer.WriteString(")VALUES ")

	for _, item := range transfersSlice {
		buffer.WriteString("(")
		for _, field := range d.Import.Fields {
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

	rs, err := d.Import.Db.Exec(buffer.String())

	if err != nil {
		seelog.Errorf("插入数据出错，出错索引为%v-%v,出错原因%v", start, end, err)
	}

	lines, err := rs.RowsAffected()

	if err != nil {
		seelog.Errorf("获取插入行数出错%v", err)
	}
	atomic.AddInt64(&d.ActualInsertCount, lines)

	seelog.Infof("插入的行数为%v", lines)

	<-d.Import.Ch
}
