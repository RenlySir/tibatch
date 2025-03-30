package method

import (
	"database/sql"
	"fmt"
	"github.com/spf13/tibatch/utils"
	"sync"
)

func InsertByRowID(db *sql.DB, ssql string, isql string, threadCount int) {

	rows, err := db.Query(ssql)
	utils.HandleError(err, "Error executing query")
	defer rows.Close()

	var wg sync.WaitGroup
	pages := make(chan [4]interface{}, 10) // 通道用于传递分页信息

	// 启动 worker goroutines
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for page := range pages {
				page_num := page[0].(int)
				start_key := page[1].(int64)
				end_key := page[2].(int64)
				page_size := page[3].(int)

				fmt.Printf("Page Num: %d, Start Key: %d, End Key: %d, Page Size: %d\n", page_num, start_key, end_key, page_size)

				InsertDataByPage(db, isql)
			}
		}()
	}
	// 读取分页信息并发送到通道
	for rows.Next() {
		var page_num int
		var start_key int64
		var end_key int64
		var page_size int
		utils.HandleError(rows.Scan(&page_num, &start_key, &end_key, &page_size), "Error scanning row")
		pages <- [4]interface{}{page_num, start_key, end_key, page_size}
	}

	close(pages)
	wg.Wait() // 等待所有 worker 完成
	utils.HandleError(rows.Err(), "Error iterating over rows")

}

func InsertDataByPage(db *sql.DB, isql string) {
	// 构造 INSERT ON DUPLICATE KEY UPDATE 查询
	insertQuery := fmt.Sprintf(`
        INSERT INTO %s.%s (%s)
        SELECT %s FROM %s.%s t
        WHERE t._tidb_rowid BETWEEN ? AND ?
        ON DUPLICATE KEY UPDATE %s;
    `)

	_, err := db.Exec(insertQuery, start_key, end_key)
	utils.HandleError(err, "Error inserting data")
	fmt.Printf("Data inserted for range: %d to %d\n", start_key, end_key)
}
