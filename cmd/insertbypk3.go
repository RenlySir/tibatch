/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"github.com/spf13/tibatch/method"
	"github.com/spf13/tibatch/utils"

	"github.com/spf13/cobra"
)

// insertbypkCmd represents the insertbypk command
var insertbypk3Cmd = &cobra.Command{
	Use:   "insertbypk3",
	Short: "fit table that table has 3 columns as primary key",
	Long:  `insertbypk3 where table have 3 columns as primary key`,
	Run: func(cmd *cobra.Command, args []string) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUser, dbPassword, dbHost, dbPort)
		db, err := utils.GetDBConnectionPool(dsn)
		utils.HandleError(err, "Error connecting to MySQL")
		defer db.Close()
		// 调用方法插入数据
		method.InsertByPK3(db, databaseName, tableName, targetDatabaseName, targetTableName,
			primaryKeyColumns, selectColumns, pageNumber, threadCount, whereCondition)
	},
}

func init() {
	rootCmd.AddCommand(insertbypk3Cmd)

}
