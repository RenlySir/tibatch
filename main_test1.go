package main

import (
	"fmt"
	"strings"
)

func insert() {
	//ssql := "select distinct(name) from db1.t1 "
	isql := "insert into db2.t1 l1 left join db2.t2 l2 on l1.id=l2.id where l1.name=? and l2.name=? and l1.col=? and l2.col=? "
	insertPeriodData(isql)
}

func insertPeriodData(isql string) {
	count := strings.Count(isql, "?")
	period := "202201"
	for i := 0; i < count; i++ {
		isql = strings.Replace(isql, "?", fmt.Sprintf("%s", period), 1)
	}

	fmt.Printf("打印最终的sql: %s", isql)
	//_, err := db.Exec(query, period)
	//utils.HandleError(err, "执行插入失败:")
}
