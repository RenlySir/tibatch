package cmd

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"github.com/spf13/tibatch/utils"
)

var mysqlCmd = &cobra.Command{
	Use:   "showdbs",
	Short: "Show MySQL databases",
	Long:  `Connect to MySQL and execute 'SHOW DATABASES' command.`,
	Run: func(cmd *cobra.Command, args []string) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", dbUser, dbPassword, dbHost, dbPort)

		db, err := utils.GetDBConnectionPool(dsn)
		utils.HandleError(err, "Error connecting to MySQL")
		defer db.Close()
		rows, err := db.Query("SHOW DATABASES")
		utils.HandleError(err, "Error executing query")
		defer rows.Close()

		// 打印结果
		fmt.Println("Databases:")
		for rows.Next() {
			var dbName string
			utils.HandleError(rows.Scan(&dbName), "Error scanning row")
			fmt.Println(dbName)
		}

		utils.HandleError(rows.Err(), "Error iterating over rows")
	},
}

func init() {

	rootCmd.AddCommand(mysqlCmd)

	mysqlCmd.Flags().StringVarP(&dbUser, "user", "u", "root", "TiDB username")
	mysqlCmd.Flags().StringVarP(&dbPassword, "password", "p", "", "TiDB password")
	mysqlCmd.Flags().StringVarP(&dbHost, "host", "H", "127.0.0.1", "TiDB host")
	mysqlCmd.Flags().StringVarP(&dbPort, "port", "P", "3306", "TiDB port")
}
