package utils

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func GetDBConnectionPool(dsn string) (*sql.DB, error) {

	db, err := sql.Open("mysql", dsn)
	HandleError(err, "Error connecting to MySQL")

	// 设置连接池参数
	db.SetMaxOpenConns(100)   // 最大打开的连接数
	db.SetMaxIdleConns(10)    // 最大空闲连接数
	db.SetConnMaxLifetime(60) // 连接的最大生命周期（秒）

	HandleError(db.Ping(), "Error pinging MySQL")
	
	return db, nil
}
