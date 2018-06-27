package core

import (
	"sync"

	"fmt"

	"github.com/cihub/seelog"
)

func (d *Deliver) Beigin(inits ...func()) {

	d.InitBefore()

	for _, init := range inits {
		init()
	}

	d.InitAfter()
	d.Run()
}

func (d *Deliver) Run() {

	d.Scheduler.Run()
	d.Scheduler.WorkerLimit(5, 5000)
	d.ProcessNumber = 5
	for i := 0; i < int(d.ProcessNumber); i++ {
		seelog.Infof("插入线程%v开启", i+1)
		d.CreateInsertWorker()
	}

	d.ExportDispatch()

}

func (d *Deliver) CreateInsertWorker() {
	in := d.Scheduler.WorkerChan()

	go func() {

		for {

			d.Scheduler.WorkerReady(in)

			insertDatas := <-in

			fmt.Println(len(*insertDatas.Data))

			d.Insert(insertDatas)
		}
	}()
}

func (d *Deliver) InitBefore() {

	d.Export.FieldsValue = make(map[string]interface{})

	d.Attach.Chs = make(map[string]chan struct{})
	d.Attach.Data = make(map[string]*sync.Map)

	d.Import = make(map[string]*DbInfo)

}

//将导出数据库的数据进行导出，并开始将老数据赋值给相应的新表字段

func (d *Deliver) InitAfter() {

	if d.Export.Ch == nil {
		d.Export.Ch = make(chan struct{}, 5)
	}

	if d.Export.GoroutineNumber == 0 {
		d.Export.GoroutineNumber = 50000.00

	}
	for _, field := range d.Export.Fields {
		d.Export.FieldsValue[field] = new([]byte)
	}

	for _, db := range d.Import {
		if db.Ch == nil {
			db.Ch = make(chan struct{}, 5)
		}
	}

	for _, db := range d.Import {
		if db.GoroutineNumber == 0 {
			db.GoroutineNumber = 5000.00
		}
	}

	//声明导入表的导入字段
	for _, db := range d.Import {
		db.Fields = make([]string, 0, len(db.FieldsValue))
	}

	// 获取每个表导入的字段名，并且将字段对应值类型全部改成ValueFunc

	for _, db := range d.Import {
		for key, value := range db.FieldsValue {

			db.Fields = append(db.Fields, key)

			vf, ok := value.(func(row map[string]*[]byte) string)
			if ok {
				db.FieldsValue[key] = ValueFunc(vf)
			}
		}
	}

}

func (d *Deliver) GetActualCount() {

	for _, importTable := range d.Import {
		seelog.Infof("%s实际插入的记录总量为：%v", importTable.Table, importTable.ActualInsertCount)
	}
}
