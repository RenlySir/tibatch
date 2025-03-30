package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/tibatch/utils"
)

// 定义全局变量
var (
	dbUser             string
	dbPassword         string
	dbHost             string
	dbPort             string
	databaseName       string
	targetDatabaseName string
	tableName          string
	targetTableName    string
	primaryKeyColumns  string
	selectColumns      string
	whereCondition     string
	pageSize           int
	pageNumber         int
	threadCount        int
)

// rootCmd 代表根命令
var rootCmd = &cobra.Command{
	Use:   "tibatch-cli",
	Short: "A simple CLI tool to interact with TiDB",
	Long:  `This tool provides commands to perform various operations on TiDB.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		utils.HandleError(err, "Error executing root command")
	}
}

func init() {
	// 定义全局参数
	rootCmd.PersistentFlags().StringVarP(&dbUser, "user", "u", "root", "TiDB username")
	rootCmd.PersistentFlags().StringVarP(&dbPassword, "password", "p", "", "TiDB password")
	rootCmd.PersistentFlags().StringVarP(&dbHost, "host", "H", "127.0.0.1", "TiDB host")
	rootCmd.PersistentFlags().StringVarP(&dbPort, "port", "P", "4000", "TiDB port")
	rootCmd.PersistentFlags().StringVarP(&databaseName, "databaseName", "d", "", "Database name")
	rootCmd.PersistentFlags().StringVarP(&targetDatabaseName, "targetdatabaseName", "k", "tpcc", "Target database name")
	rootCmd.PersistentFlags().StringVarP(&tableName, "tableName", "t", "", "Table name")
	rootCmd.PersistentFlags().StringVarP(&targetTableName, "targettableName", "b", "", "Target Table name")
	rootCmd.PersistentFlags().StringVarP(&primaryKeyColumns, "primarykeys", "z", "", "Primary key columns (comma-separated)")
	rootCmd.PersistentFlags().StringVarP(&selectColumns, "selectcolumns", "x", "*", "Columns to select (comma-separated)")
	rootCmd.PersistentFlags().StringVarP(&whereCondition, "whereCondition", "w", "", "give table where condition if needed ")
	rootCmd.PersistentFlags().IntVarP(&pageSize, "pagesize", "s", 5000, "Page size")
	rootCmd.PersistentFlags().IntVarP(&pageNumber, "pagenumber", "n", 1, "Page number")
	rootCmd.PersistentFlags().IntVarP(&threadCount, "threads", "c", 8, "Number of threads")

}
