/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/tibatch/method"
	"github.com/spf13/tibatch/utils"
)

var (
	sourceDatabaseName string
	sourceTableName    string
)

// insert2bypk3Cmd represents the insert2bypk3 command
var insert2bypk3Cmd = &cobra.Command{
	Use:   "insert2bypk3",
	Short: "fit 2 table left join insert into another table",
	Long:  `insert2bypk3 table has 3 columns as primary key, and 2 table left join insert into another table`,
	Run: func(cmd *cobra.Command, args []string) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUser, dbPassword, dbHost, dbPort)
		db, err := utils.GetDBConnectionPool(dsn)
		utils.HandleError(err, "Error connecting to TiDB")
		defer db.Close()
		// 调用方法插入数据
		method.Insert2ByPK23(db, databaseName, tableName, targetDatabaseName, targetTableName, primaryKeyColumns,
			selectColumns, pageNumber, threadCount, whereCondition, sourceDatabaseName, sourceTableName)
	},
}

func init() {
	rootCmd.AddCommand(insert2bypk3Cmd)
	insert2bypk3Cmd.PersistentFlags().StringVarP(&sourceDatabaseName, "sourceDatabaseName", "q", "", "source Database name 2")
	insert2bypk3Cmd.PersistentFlags().StringVarP(&sourceTableName, "sourceTableName", "a", "", "source table name 2")
}
