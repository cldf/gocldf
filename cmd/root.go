package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "gocldf",
	Short: "gocldf is a cli tool to handle CLDF datasets",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Oops. An error while executing gocldf '%s'\n", err)
		os.Exit(1)
	}
}
