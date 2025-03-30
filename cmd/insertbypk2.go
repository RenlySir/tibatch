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

// insertbypk2Cmd represents the insertbypk2 command
var insertbypk2Cmd = &cobra.Command{
	Use:   "insertbypk2",
	Short: "fit table that table has 2 columns as primary key",
	Long:  `insertbypk2 where table have 2 columns as primary key`,
	Run: func(cmd *cobra.Command, args []string) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUser, dbPassword, dbHost, dbPort)
		db, err := utils.GetDBConnectionPool(dsn)
		utils.HandleError(err, "Error connecting to MySQL")
		defer db.Close()
		// 调用方法插入数据
		method.InsertByPK2(db, databaseName, tableName, targetDatabaseName, targetTableName, primaryKeyColumns,
			selectColumns, pageNumber, threadCount, whereCondition)

	},
}

func init() {
	rootCmd.AddCommand(insertbypk2Cmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// insertbypk2Cmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// insertbypk2Cmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
