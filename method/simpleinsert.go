package method

import (
	"database/sql"
	"fmt"
	"github.com/spf13/tibatch/utils"
	"strings"
	"sync"
)

func insert(db *sql.DB, ssql string, isql string) {
	rows, err := db.Query(ssql)
	utils.HandleError(err, "Error executing query ssql :")
	defer rows.Close()
	for rows.Next() {
		var period string
		if err := rows.Scan(&period); err != nil {
			utils.HandleError(err, "扫描结果失败: %v")
		}
		insertPeriodData(db, isql, period)
	}
	if err = rows.Err(); err != nil {
		utils.HandleError(err, "遍历结果失败: %v")
	}
}

func insertPeriodData(db *sql.DB, isql string, period string) {
	count := strings.Count(isql, "?")

	for i := 0; i < count; i++ {
		isql = strings.Replace(isql, "?", fmt.Sprintf("%s", period), 1)
	}

	query := fmt.Sprintf(`
		isql`)
	fmt.Printf("打印最终的sql: %s", query)
	_, err := db.Exec(query, period)
	utils.HandleError(err, "执行插入失败:")
}

func BatchProcess(db *sql.DB, ssql string, isql string, threadCount int) {
	var wg sync.WaitGroup
	// 启动工作协程
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			insert(db, ssql, isql)
		}()
	}

}
