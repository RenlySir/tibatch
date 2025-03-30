/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>

	./mysql-cli insertbyrowid -u root -p '' -H 113.44.138.199 -P 4000 -d 'tpcc' -t 'customer1'  -s 50000 -k 'tpcc' -b 'c3' -c 64 -z "c_w_id,c_d_id,c_id"  -x "c_id,c_w_id,c_d_id,c_data"
*/
package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/tibatch/method"
	"github.com/spf13/tibatch/utils"
)

var insertbyrowidCmd = &cobra.Command{
	Use:   "insertbyrowid",
	Short: "fit table that table has no  primary key",
	Long:  `insertbypk2 where table has no primary key`,
	Run: func(cmd *cobra.Command, args []string) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUser, dbPassword, dbHost, dbPort)

		db, err := utils.GetDBConnectionPool(dsn)
		utils.HandleError(err, "Error connecting to MySQL")
		defer db.Close()

		// 调用方法插入数据
		method.InsertByRowID(db, ssql, isql, threadCount)

	},
}

func init() {
	rootCmd.AddCommand(insertbyrowidCmd)
}
