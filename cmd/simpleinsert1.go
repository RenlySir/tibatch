/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

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
		fmt.Println("simpleupsert1 called")
		//1、传入分页的SQL
		//2、传入写入表的SQL
	},
}

func init() {
	rootCmd.AddCommand(simpleinsert1Cmd)
	simpleinsert1Cmd.PersistentFlags().StringVarP(&ssql, "ssql", "e", "", "分区查询的SQL")
	simpleinsert1Cmd.PersistentFlags().StringVarP(&isql, "isql", "f", "", "写入数据的SQL")
}
