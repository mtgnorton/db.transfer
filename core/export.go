package core

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"

	"sync/atomic"

	"db.transfer/helper"
	"github.com/cihub/seelog"
)

func (d *Deliver) ExportDispatch() {

	sqlQuery := "select count(*) from " + d.Export.Table

	var number float64

	err := d.Export.Db.QueryRow(sqlQuery).Scan(&number)

	if err != nil {
		seelog.Errorf("%v查询出错%v", d.Export.Table, err)
	}

	readGoroutineNumber := math.Ceil(number / d.Export.GoroutineNumber)

	GoroutineExportNumber := int(d.Export.GoroutineNumber)

	seelog.Infof("导出的数量为%v", number)

	seelog.Infof("开启的读线程数量为%v", readGoroutineNumber)

	seelog.Infof("每个线程读取的数量为%v", GoroutineExportNumber)

	var wg sync.WaitGroup

	for i := 0; i < int(readGoroutineNumber); i++ {

		startId := i * GoroutineExportNumber

		endId := (i + 1) * GoroutineExportNumber

		d.Export.Ch <- struct{}{}

		wg.Add(1)

		seelog.Infof("读取线程%v开启", i+1)

		go func() {
			d.ExportTable(startId, endId)

			defer func() {

				seelog.Infof("读取线程%v完成", i+1)

				<-d.Export.Ch

				wg.Done()
			}()
		}()

	}

	wg.Wait()

	seelog.Infof("导出数量为%v", len(d.InsertData))

}

func (d *Deliver) ExportTable(startId, endId int) {

	sqlQuery := "select"

	exportFieldsCopy := helper.Copy(d.Export.Fields)

	var ExportFieldsSlice = make([]interface{}, 0, len(exportFieldsCopy))

	for key, _ := range exportFieldsCopy {
		sqlQuery += (" `" + key + "` ,")
		ExportFieldsSlice = append(ExportFieldsSlice, exportFieldsCopy[key])
	}

	sqlQueryByte := []byte(sqlQuery)

	sqlQuery = string(sqlQueryByte[:len(sqlQueryByte)-1])

	sqlQuery += ("from " + d.Export.Table)

	sqlQuery += (" where id > " + strconv.Itoa(startId) + " and id < " + strconv.Itoa(endId))

	if d.Test.Open {
		sqlQuery += " limit 20"
	}

	if d.Test.Open && d.Test.HasOutput == 0 {
		atomic.AddInt32(&d.Test.HasOutput, 1)
		seelog.Info(sqlQuery)
	}
	exportRs, err := d.Export.Db.Query(sqlQuery)
	if err != nil {
		seelog.Errorf("%v查询出错%v", d.ExportTable, err)
		fmt.Printf("%v查询出错%v", d.ExportTable, err)
	}

	//待前置操作完成
	for _, item := range d.Attach.Chs {
		<-item
	}
	var importData []map[string]string

	for exportRs.Next() {

		if err := exportRs.Scan(ExportFieldsSlice...); err != nil {
			seelog.Errorf("%v读取数据出错%v", d.ExportTable, err)
			fmt.Printf("%v读取数据出错%v", d.ExportTable, err)
		}

		rowData := make(map[string]string)

		for _, key := range d.Import.Fields {
			valueFunc, ok := d.Import.FieldsValue[key].(ValueFunc)
			if ok {
				rowData[key] = valueFunc(exportFieldsCopy)
			} else {
				seelog.Errorf("字段值函数断言错误")
				seelog.Flush()
				os.Exit(1)
			}

			//如果返回continue，将跳过该条记录
			if rowData[key] == "continue" {
				goto stop
			}

		}

		importData = append(importData, rowData)
	stop:
	}

	exportRs.Close()

	d.Lock()

	d.InsertData = append(d.InsertData, importData...)

	d.Unlock()

	seelog.Infof("id从%v到%v读取完成", startId, endId)

	if err = exportRs.Err(); err != nil {

		seelog.Errorf("遍历数据库数据时出错，出错id为%v-%v", startId, endId)

	}

}
