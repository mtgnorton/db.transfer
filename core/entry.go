package core

import "sync"

func (d *Deliver) Beigin(inits ...func()) {

	d.InitBefore()

	for _, init := range inits {
		init()
	}

	d.InitAfter()
}

func (d *Deliver) InitBefore() {

	d.Export.FieldsValue = make(map[string]interface{})

	d.Attach.Chs = make(map[string]chan struct{})
	d.Attach.Data = make(map[string]*sync.Map)

	if d.Import.Ch == nil {
		d.Import.Ch = make(chan struct{}, 5)

	}
	if d.Export.Ch == nil {
		d.Export.Ch = make(chan struct{}, 5)
	}

	if d.Import.GoroutineNumber == 0 {
		d.Import.GoroutineNumber = 5000.00

	}

	if d.Export.GoroutineNumber == 0 {
		d.Export.GoroutineNumber = 50000.00

	}

}

//将导出数据库的数据进行导出，并开始将老数据赋值给相应的新表字段

func (d *Deliver) InitAfter() {

	for _, field := range d.Export.Fields {
		d.Export.FieldsValue[field] = new([]byte)
	}
	d.Import.Fields = make([]string, 0, len(d.Import.FieldsValue))

	for key, value := range d.Import.FieldsValue {
		vf, ok := value.(func(row map[string]*[]byte) string)
		if ok {
			d.Import.FieldsValue[key] = ValueFunc(vf)
		}
		d.Import.Fields = append(d.Import.Fields, key)
	}
}

func (d *Deliver) GetActualCount() int64 {
	return d.ActualInsertCount
}
