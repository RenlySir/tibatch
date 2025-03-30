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
	insertsql string
)

// simpleupsert1Cmd represents the simpleupsert1 command
var simpleinsert2Cmd = &cobra.Command{
	Use:   "simpleinsert2",
	Short: "give 2 sql",
	Long:  `page sql and insert into sql `,
	Run: func(cmd *cobra.Command, args []string) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUser, dbPassword, dbHost, dbPort)
		db, err := utils.GetDBConnectionPool(dsn)
		utils.HandleError(err, "Error connecting to TiDB")
		defer db.Close()
		method.BatchProcess2(db, databaseName, tableName, pageSize, threadCount, insertsql, whereCondition)
	},
}

func init() {
	rootCmd.AddCommand(simpleinsert2Cmd)
	simpleinsert2Cmd.PersistentFlags().StringVarP(&insertsql, "insertsql", "e", "", "写入数据的SQL")
}
