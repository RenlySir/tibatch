package method

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
)

// PageMeta 分页元数据
type PageMeta3 struct {
	PageNum  int
	StartKey string
	EndKey   string
	PageSize int
	FirstWID int
	LastWID  int
	FirstDID int
	LastDID  int
	FirstCID int
	LastCID  int
}

// InsertByPK 根据分页的 start_key 和 end_key 插入数据到目标表
func InsertByPK3(db *sql.DB, databaseName string, tableName string, targetDatabaseName string, targetTableName string,
	primaryKeyColumns string, selectColumns string, pageSize int, threadCount int, whereCondition string) {
	// 步骤1: 获取分页元数据
	pages := getPageMetadata3(db, primaryKeyColumns, pageSize, databaseName, tableName, whereCondition)
	// 步骤2: 并发处理分页
	processPages3(db, pages, threadCount, databaseName, tableName, targetDatabaseName, targetTableName, primaryKeyColumns, selectColumns)
}

// 获取分页元数据
func getPageMetadata3(db *sql.DB, primaryKeyColumns string, pageSize int, databaseName string, tableName string, whereCondition string) []PageMeta3 {
	// 直接切割主键列（简单处理，不验证格式）
	pkCols := strings.Split(primaryKeyColumns, ",")
	for i := range pkCols {
		pkCols[i] = strings.TrimSpace(pkCols[i])
	}

	// 硬编码SQL模板（注意SQL注入风险！）
	query := fmt.Sprintf(`
		SELECT
			floor((t1.row_num - 1) / %d) + 1 AS page_num,
			MIN(mvalue) AS start_key,
			MAX(mvalue) AS end_key,
			COUNT(*) AS page_size,
			MIN(%s), MAX(%s),
			MIN(%s), MAX(%s),
			MIN(%s), MAX(%s)
		FROM (
			SELECT 
				CONCAT('(', 
					LPAD(%s, 19, '0'), ',',
					LPAD(%s, 19, '0'), ',',
					LPAD(%s, 19, '0'), ')'
				) AS mvalue,
				%s, %s, %s,
				ROW_NUMBER() OVER(ORDER BY %s, %s, %s) AS row_num
			FROM %s.%s %S
		) t1
		GROUP BY page_num
		ORDER BY page_num`,
		pageSize,
		pkCols[0], pkCols[0],
		pkCols[1], pkCols[1],
		pkCols[2], pkCols[2],
		pkCols[0], pkCols[1], pkCols[2], // CONCAT部分
		pkCols[0], pkCols[1], pkCols[2], // SELECT原始列
		pkCols[0], pkCols[1], pkCols[2], // ORDER BY
		databaseName, tableName, whereCondition,
	)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("分页查询失败:", err)
	}
	defer rows.Close()

	var pages []PageMeta3
	for rows.Next() {
		var pm PageMeta3
		err := rows.Scan(
			&pm.PageNum, &pm.StartKey, &pm.EndKey, &pm.PageSize,
			&pm.FirstWID, &pm.LastWID,
			&pm.FirstDID, &pm.LastDID,
			&pm.FirstCID, &pm.LastCID,
		)
		if err != nil {
			log.Fatal("解析分页数据失败:", err)
		}
		pages = append(pages, pm)
	}
	return pages
}

// 并发处理分页
func processPages3(db *sql.DB, pages []PageMeta3, threadCount int, databaseName string, tableName string,
	targetDatabaseName string, targetTableName string, primaryKeyColumns string,
	selectColumns string) {
	ch := make(chan PageMeta3, threadCount)
	var wg sync.WaitGroup

	// 启动工作协程
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for page := range ch {
				insertPage3(db, page, databaseName, tableName, targetDatabaseName, targetTableName,
					primaryKeyColumns, selectColumns)
			}
		}()
	}

	// 发送任务
	for _, page := range pages {
		ch <- page
	}
	close(ch)
	wg.Wait()
}

// 构建WHERE条件
func buildWhereClause3(page PageMeta3, primaryKeyColumns string) string {
	// 解析起始结束值
	startVals := parseKey(page.StartKey) // [w, d, c]
	endVals := parseKey(page.EndKey)     // [w, d, c]

	pks := strings.Split(primaryKeyColumns, ",")
	for i := range pks {
		// 移除前导零并转换为数字
		pks[i] = strings.TrimLeft(pks[i], "0")
		if pks[i] == "" {
			pks[i] = "0"
		}
	}

	var conditions []string

	// 情况1: w_id在中间范围
	conditions = append(conditions, fmt.Sprintf(
		"(pks[0] > %s AND pks[0] < %s)",
		startVals[0], endVals[0],
	))

	// 情况2: w_id等于起始值
	conditions = append(conditions, fmt.Sprintf(
		"(pks[0] = %s AND pks[1] > %s)",
		startVals[0], startVals[1],
	))
	conditions = append(conditions, fmt.Sprintf(
		"(pks[0] = %s AND pks[1] = %s AND pks[2] >= %s)",
		startVals[0], startVals[1], startVals[2],
	))

	// 情况3: w_id等于结束值
	conditions = append(conditions, fmt.Sprintf(
		"(pks[0] = %s AND pks[1] < %s)",
		endVals[0], endVals[1],
	))
	conditions = append(conditions, fmt.Sprintf(
		"(pks[0] = %s AND pks[1] = %s AND pks[2] <= %s)",
		endVals[0], endVals[1], endVals[2],
	))

	return "(" + strings.Join(conditions, " OR ") + ")"
}

func insertPage3(db *sql.DB, page PageMeta3, databaseName string, tableName string, targetDatabaseName string,
	targetTableName string, primaryKeyColumns string, selectColumns string) {

	whereClause := buildWhereClause3(page, primaryKeyColumns)
	query := fmt.Sprintf(`
		INSERT INTO %s.%s
		SELECT %s FROM %s.%s
		WHERE %s
		ON DUPLICATE KEY UPDATE 
			%s`, targetDatabaseName, targetTableName, selectColumns, databaseName, tableName,
		whereClause, generateUpdateClause(primaryKeyColumns))

	//fmt.Printf("拼接的SQL：", query)
	_, err := db.Exec(query)
	if err != nil {
		log.Printf("插入分页%d失败: %v", page.PageNum, err)
		return
	}
	log.Printf("成功插入分页%d, 行数%d", page.PageNum, page.PageSize)
}
