package main

import (
	"database/sql"
	"strings"
	"sync"

	"fmt"

	"db.transfer/core"

	"github.com/cihub/seelog"
)
import (
	"db.transfer/helper"
	_ "github.com/go-sql-driver/mysql"
)

type optimize struct {
	core.Deliver
}

func (d *optimize) Init() {
	//db, err := sql.Open("mysql", "wwwbtcblcom:39p2AtWeJNvoXkICDXAy@tcp(47.74.159.140:3306)/wwwbtcblcom")
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/old_bilian")
	if err != nil {
		seelog.Errorf("打开数据库出错%v", err)
	}

	d.Export.Db = db

	//db, err = sql.Open("mysql", "ceshi1btcblcom:hFKMObz5WYcT1BgZ0BPE@tcp(47.74.159.140:3306)/ceshi1btcblcom")
	db, err = sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/new_bilian")
	if err != nil {
		seelog.Errorf("打开数据库出错%v", err)
	}

	d.Import.Db = db

	d.Export.Table = "dms_货币交易交易"
	d.Import.Table = "coin_deal_orders_match"

	d.Export.Field = []string{
		"买入委托",
		"买入编号",
		"卖出编号",
		"卖出委托",
		"价格",
		"数量",
		"买入手续费金额",
		"交易时间",
		"账户",
		"货币类型",
		"卖出手续费金额",
	}

	//d.Test = true
	//d.Predict = true
	d.Attach.Chs["user"] = make(chan struct{})

	d.Attach.Data["user"] = &sync.Map{}

	d.Attach.Chs["symbols"] = make(chan struct{})

	d.Attach.Data["symbols"] = &sync.Map{}

}

func (d *optimize) InitDefaultData() {

	d.Import.FieldValue = map[string]interface{}{

		"symbol_id": func(row map[string]*[]byte) string {

			name := string(*row["账户"]) + string(*row["货币类型"])
			symbolId, ok := d.Attach.Data["symbols"].Load(name)
			if !ok {
				d.NoInsertCount++
				return "continue"
			}

			return fmt.Sprintf("%q", symbolId)
		},
		"order_id": core.Value("买入委托", ""),

		"order_uid": func(row map[string]*[]byte) string {

			userid, ok := d.Attach.Data["user"].Load(string(*row["买入编号"]))
			if !ok {
				d.NoInsertCount++
				return "continue"
			}
			return fmt.Sprintf("%q", userid)
		},

		"match_id": core.Value("卖出委托", ""),
		"match_uid": func(row map[string]*[]byte) string {
			userid, ok := d.Attach.Data["user"].Load(string(*row["卖出编号"]))
			if !ok {
				d.NoInsertCount++
				return "continue"

			}
			return fmt.Sprintf("%q", userid)
		},
		"price":         core.Value("价格", ""),
		"filled_amount": core.Value("数量", ""),
		"filled_fees":   core.Value("买入手续费金额", ""),
		"create_time":   core.Value("交易时间", ""),
		"status":        core.Value("", "1"),
		"direction":     core.Value("", "0"),
	}

}

func (d *optimize) GetUserInfo() {

	users, err := d.Export.Db.Query("select `id`,`编号` from dms_会员")
	if err != nil {
		seelog.Errorf("读取会员出错%v", err)
	}
	var count int
	for users.Next() {
		var username string
		var id []byte
		if err := users.Scan(&id, &username); err != nil {
			seelog.Errorf("读取数据出错dms_会员 %v", err)
		}

		count++
		d.Attach.Data["user"].Store(username, string(id))
	}
	seelog.Infof("用户总量为：%v", count)
	close(d.Attach.Chs["user"])
}
func (d *optimize) GetSymbolInfo() {

	users, err := d.Import.Db.Query("select `id`,`name` from coin_symbol")
	if err != nil {

		seelog.Errorf("读取coin_symbol出错%v", err)
		fmt.Printf("读取coin_symbol出错%v", err)
	}
	var count int
	for users.Next() {
		var name string
		var id []byte
		if err := users.Scan(&id, &name); err != nil {
			seelog.Errorf("读取coin_symbol数据出错 %v", err)
		}

		count++
		d.Attach.Data["symbols"].Store(strings.ToLower(name), string(id))
	}
	seelog.Infof("symbol总量为：%v", count)
	close(d.Attach.Chs["symbols"])
}

func main() {
	helper.SetLogger("logConfig.xml")
	defer func() {
		seelog.Flush()
	}()

	seelog.Info("开始插入")
	d := optimize{}

	d.Beigin(d.Init, d.InitDefaultData)

	go d.GetSymbolInfo()

	go d.GetUserInfo()

	d.ExportDispatch()

	d.ImportDispatch()

	seelog.Infof("实际插入的记录总量为：%v", d.GetActualCount())

	seelog.Info("插入结束")

}
