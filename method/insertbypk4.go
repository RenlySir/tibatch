package method

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
)

// PageMeta 分页元数据
type PageMeta4 struct {
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
	FirstXID int
	LastXID  int
}

// InsertByPK 根据分页的 start_key 和 end_key 插入数据到目标表
func InsertByPK4(db *sql.DB, databaseName string, tableName string, targetDatabaseName string, targetTableName string, primaryKeyColumns string, selectColumns string, pageSize int, threadCount int, whereCondition string) {
	// 步骤1: 获取分页元数据
	pages := getPageMetadata4(db, primaryKeyColumns, pageSize, databaseName, tableName, whereCondition)
	// 步骤2: 并发处理分页
	processPages4(db, pages, threadCount, databaseName, tableName, targetDatabaseName, targetTableName, primaryKeyColumns, selectColumns)
}

// 获取分页元数据
func getPageMetadata4(db *sql.DB, primaryKeyColumns string, pageSize int, databaseName string, tableName string, whereCondition string) []PageMeta4 {
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
			MIN(%s), MAX(%s),
			MIN(%s), MAX(%s),
		FROM (
			SELECT 
				CONCAT('(', 
					LPAD(%s, 19, '0'), ',',
					LPAD(%s, 19, '0'), ',',
					LPAD(%s, 19, '0'), ',',
					LPAD(%s, 19, '0'), ')'
				) AS mvalue,
				%s, %s, %s, %s,
				ROW_NUMBER() OVER(ORDER BY %s, %s, %s, %s,) AS row_num
			FROM %s.%s %s
		) t1
		GROUP BY page_num
		ORDER BY page_num`,
		pageSize,
		pkCols[0], pkCols[0],
		pkCols[1], pkCols[1],
		pkCols[2], pkCols[2],
		pkCols[3], pkCols[3],
		pkCols[0], pkCols[1], pkCols[2], pkCols[3], // CONCAT部分
		pkCols[0], pkCols[1], pkCols[2], pkCols[3], // SELECT原始列
		pkCols[0], pkCols[1], pkCols[2], pkCols[3], // ORDER BY
		databaseName, tableName, whereCondition,
	)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("分页查询失败:", err)
	}
	defer rows.Close()

	var pages []PageMeta4
	for rows.Next() {
		var pm PageMeta4
		err := rows.Scan(
			&pm.PageNum, &pm.StartKey, &pm.EndKey, &pm.PageSize,
			&pm.FirstWID, &pm.LastWID,
			&pm.FirstDID, &pm.LastDID,
			&pm.FirstCID, &pm.LastCID,
			&pm.FirstXID, &pm.LastXID,
		)
		if err != nil {
			log.Fatal("解析分页数据失败:", err)
		}
		pages = append(pages, pm)
	}
	return pages
}

// 并发处理分页
func processPages4(db *sql.DB, pages []PageMeta4, threadCount int, databaseName string, tableName string,
	targetDatabaseName string, targetTableName string, primaryKeyColumns string,
	selectColumns string) {
	ch := make(chan PageMeta4, threadCount)
	var wg sync.WaitGroup

	// 启动工作协程
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for page := range ch {
				insertPage4(db, page, databaseName, tableName, targetDatabaseName, targetTableName, primaryKeyColumns, selectColumns)
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
func buildWhereClause4(page PageMeta4, primaryKeyColumns string) string {
	// 解析起始结束值
	startVals := parseKey(page.StartKey) // [w, d, c, e]
	endVals := parseKey(page.EndKey)     // [w, d, c, e]

	pkCols := strings.Split(primaryKeyColumns, ",")
	for i := range pkCols {
		pkCols[i] = strings.TrimSpace(pkCols[i])
	}

	var conditions []string
	// 情况1: w_id在中间范围
	conditions = append(conditions, fmt.Sprintf(
		"(pkCols[0] > %s AND pkCols[0]  < %s)",
		startVals[0], endVals[0],
	))

	// 情况2: w_id等于起始值
	conditions = append(conditions, fmt.Sprintf(
		"(pkCols[0]  = %s AND pkCols[1]  > %s)",
		startVals[0], startVals[1],
	))
	conditions = append(conditions, fmt.Sprintf(
		"(pkCols[0]  = %s AND pkCols[1] = %s AND pkCols[2] > %s)",
		startVals[0], startVals[1], startVals[2],
	))
	conditions = append(conditions, fmt.Sprintf(
		"(pkCols[0]  = %s AND pkCols[1] = %s AND pkCols[2] = %s AND pkCols[3] >= %s)",
		startVals[0], startVals[1], startVals[2], startVals[3],
	))

	// 情况3: w_id等于结束值
	conditions = append(conditions, fmt.Sprintf(
		"(pkCols[0]  = %s AND pkCols[1] < %s)",
		endVals[0], endVals[1],
	))
	conditions = append(conditions, fmt.Sprintf(
		"(pkCols[0]  = %s AND pkCols[1] = %s AND pkCols[2] < %s)",
		endVals[0], endVals[1], endVals[2],
	))
	conditions = append(conditions, fmt.Sprintf(
		"(pkCols[0]  = %s AND pkCols[1] = %s AND pkCols[2] = %s AND pkCols[3] <= %s)",
		endVals[0], endVals[1], endVals[2], endVals[3],
	))

	// 情况4: 起始和结束条件的组合
	conditions = append(conditions, fmt.Sprintf(
		"(pkCols[0]  = %s AND pkCols[1] = %s AND pkCols[2] = %s AND pkCols[3] >= %s AND pkCols[4] <= %s)",
		startVals[0], startVals[1], startVals[2], startVals[3], endVals[3],
	))

	return "(" + strings.Join(conditions, " OR ") + ")"
}

func insertPage4(db *sql.DB, page PageMeta4, databaseName string, tableName string, targetDatabaseName string,
	targetTableName string, primaryKeyColumns string, selectColumns string) {

	whereClause := buildWhereClause4(page, primaryKeyColumns)
	query := fmt.Sprintf(`
		INSERT INTO %s.%s
		SELECT %s FROM %s.%s
		WHERE %s
		ON DUPLICATE KEY UPDATE 
			%s`, targetDatabaseName, targetTableName, selectColumns, databaseName, tableName,
		whereClause, generateUpdateClause(primaryKeyColumns))

	_, err := db.Exec(query)
	if err != nil {
		log.Printf("插入分页%d失败: %v", page.PageNum, err)
		return
	}
	log.Printf("成功插入分页%d, 行数%d", page.PageNum, page.PageSize)
}
