package utils

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func GetDBConnection(dsn string) (*sql.DB, error) {

	db, err := sql.Open("mysql", dsn)
	HandleError(err, "Error pinging MySQL")

	HandleError(db.Ping(), "Error pinging MySQL")

	return db, nil
}
