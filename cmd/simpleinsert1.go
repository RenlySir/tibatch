package cmd

import (
	"fmt"
	"github.com/spf13/tibatch/method"
	"github.com/spf13/tibatch/utils"

	"github.com/spf13/cobra"
)

var (
	ssql string
	isql string
)

// simpleupsert1Cmd represents the simpleupsert1 command
var simpleinsert1Cmd = &cobra.Command{
	Use:   "simpleinsert1",
	Short: "give 2 sql",
	Long:  `page sql and insert into sql `,
	Run: func(cmd *cobra.Command, args []string) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUser, dbPassword, dbHost, dbPort)
		db, err := utils.GetDBConnectionPool(dsn)
		utils.HandleError(err, "Error connecting to TiDB")

		method.BatchProcess(db, ssql, isql, threadCount)
	},
}

func init() {
	rootCmd.AddCommand(simpleinsert1Cmd)
	simpleinsert1Cmd.PersistentFlags().StringVarP(&ssql, "ssql", "e", "", "分区查询的SQL")
	simpleinsert1Cmd.PersistentFlags().StringVarP(&isql, "isql", "f", "", "写入数据的SQL")
}
