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
	"time"

	"db.transfer/helper"
	"db.transfer/scheduler"
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

	//要导入的表
	d.Import["match"] = &core.DbInfo{}
	d.Import["match"].Db = db
	d.Import["match"].Table = "coin_deal_orders_match"

	d.Import["test"] = &core.DbInfo{}
	d.Import["test"].Db = db
	d.Import["test"].Table = "test"

	d.Export.Table = "dms_货币交易交易"
	d.Export.Fields = []string{
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
		"id",
	}

	//d.Test.Open = true

	//附加的数据
	d.Attach.Chs["user"] = make(chan struct{})

	d.Attach.Data["user"] = &sync.Map{}

	d.Attach.Chs["symbols"] = make(chan struct{})

	d.Attach.Data["symbols"] = &sync.Map{}

}

func (d *optimize) InitDefaultData() {

	//
	d.Import["match"].FieldsValue = map[string]interface{}{

		"id": core.Value("id", ""),

		"symbol_id": func(row map[string]*[]byte) string {

			name := string(*row["账户"]) + string(*row["货币类型"])
			symbolId, ok := d.Attach.Data["symbols"].Load(name)
			if !ok {
				d.Import["match"].NoInsertCount++
				return "continue"
			}

			return fmt.Sprintf("%q", symbolId)
		},
		"order_id": core.Value("买入委托", ""),

		"order_uid": func(row map[string]*[]byte) string {

			userid, ok := d.Attach.Data["user"].Load(string(*row["买入编号"]))
			if !ok {
				d.Import["match"].NoInsertCount++
				return "continue"
			}
			return fmt.Sprintf("%q", userid)
		},

		"match_id": core.Value("卖出委托", ""),
		"match_uid": func(row map[string]*[]byte) string {
			userid, ok := d.Attach.Data["user"].Load(string(*row["卖出编号"]))
			if !ok {
				d.Import["match"].NoInsertCount++
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

	d.Import["test"].FieldsValue = map[string]interface{}{
		"id":       core.Value("id", ""),
		"username": core.Value("买入委托", ""),
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

	users, err := d.Import["match"].Db.Query("select `id`,`name` from coin_symbol")
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

	d.Scheduler = &scheduler.QueuedScheduler{}

	go func() {
		time.Sleep(time.Second)
		go d.GetSymbolInfo()

		go d.GetUserInfo()

	}()
	d.Beigin(d.Init, d.InitDefaultData)

	seelog.Info("插入结束")

}
