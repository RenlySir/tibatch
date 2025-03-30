package method

import (
	"database/sql"
	"fmt"
	"github.com/spf13/tibatch/utils"
	"strings"
	"sync"
)

// 获取所有需要处理的period列表
func getPeriods(db *sql.DB, ssql string) ([]string, error) {
	rows, err := db.Query(ssql)
	if err != nil {
		return nil, fmt.Errorf("执行查询失败: %v", err)
	}
	defer rows.Close()

	var periods []string
	for rows.Next() {
		var period string
		if err := rows.Scan(&period); err != nil {
			return nil, fmt.Errorf("扫描结果失败: %v", err)
		}
		periods = append(periods, period)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历结果失败: %v", err)
	}
	return periods, nil
}

// 处理单个period的插入操作
func insertPeriodData(db *sql.DB, isql string, period string) {
	fmt.Printf("开始处理period: %s\n", period)
	// 替换所有?为period值
	count := strings.Count(isql, "?")
	replacedSQL := isql
	for i := 0; i < count; i++ {
		replacedSQL = strings.Replace(replacedSQL, "?", fmt.Sprintf("'%s'", period), 1)
	}

	_, err := db.Exec(replacedSQL)
	utils.HandleError(err, "执行插入失败")
	fmt.Printf("完成处理period: %s\n", period)
}

func BatchProcess(db *sql.DB, ssql string, isql string, threadCount int) {
	fmt.Println("开始批量处理...")

	// 1. 获取所有需要处理的period
	periods, err := getPeriods(db, ssql)
	utils.HandleError(err, "获取period列表失败")

	// 2. 创建任务通道
	periodChan := make(chan string, len(periods))

	// 3. 填充任务到通道
	for _, p := range periods {
		periodChan <- p
	}
	close(periodChan)

	// 4. 创建等待组
	var wg sync.WaitGroup

	// 5. 启动工作协程
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			fmt.Printf("Worker %d 启动\n", workerID)
			for p := range periodChan {
				insertPeriodData(db, isql, p)
			}
			fmt.Printf("Worker %d 结束\n", workerID)
		}(i)
	}

	// 6. 等待所有工作协程完成
	wg.Wait()
	fmt.Println("所有处理任务完成")
}
