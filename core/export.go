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

	var isLast bool

	for i := 0; i < int(readGoroutineNumber); i++ {

		startId := i * GoroutineExportNumber

		endId := (i + 1) * GoroutineExportNumber

		d.Export.Ch <- struct{}{}

		wg.Add(1)

		seelog.Infof("读取线程%v开启", i+1)

		isLast = i == int(readGoroutineNumber)-1

		fmt.Println(isLast)
		go func() {
			d.ExportTable(startId, endId, isLast)
			defer func() {

				seelog.Infof("读取线程%v完成", i+1)

				<-d.Export.Ch

				wg.Done()
			}()
		}()

	}

	wg.Wait()

	for _, importTable := range d.Import {
		seelog.Infof("导出数量为%v", len(importTable.InsertData))
	}

}

func (d *Deliver) ExportTable(startId, endId int, isLast bool) {

	//sql拼装
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

	if !isLast {
		sqlQuery += (" where id > " + strconv.Itoa(startId) + " and id < " + strconv.Itoa(endId))
	} else {
		sqlQuery += (" where id > " + strconv.Itoa(startId)) //id的大小可能大于表中数据的总数量，在最后一个线程中，不限制最大id
	}

	if d.Test.Open {
		sqlQuery += " limit 20"
	}

	//sql拼装完成

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

	//importDataTemp用于将该线程查询的数据全部查出来后，最后一起合并到总数据中
	importDataTemp := make(map[string][]map[string]string)

	for _, importTable := range d.Import {
		importDataTemp[importTable.Table] = make([]map[string]string, 0)
	}

	for exportRs.Next() {

		if err := exportRs.Scan(ExportFieldsSlice...); err != nil {
			seelog.Errorf("%v读取数据出错%v", d.ExportTable, err)
			fmt.Printf("%v读取数据出错%v", d.ExportTable, err)
		}

		rowData := make(map[string]string)

		//计算每个表需要导入的字段值
		for _, importTable := range d.Import {

			for _, key := range importTable.Fields {

				valueFunc, ok := importTable.FieldsValue[key].(ValueFunc)
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
			importDataTemp[importTable.Table] = append(importDataTemp[importTable.Table], rowData)

		}

	stop:
	}

	exportRs.Close()

	//将每个线程获取到的数据合并的总的数据中
	d.Lock()

	for _, importTable := range d.Import {

		importTable.InsertData = append(importTable.InsertData, importDataTemp[importTable.Table]...)
	}

	d.Unlock()

	seelog.Infof("id从%v到%v读取完成", startId, endId)

	if err = exportRs.Err(); err != nil {

		seelog.Errorf("遍历数据库数据时出错，出错id为%v-%v", startId, endId)

	}

}
