package cmd

import (
	"fmt"
	"gocldf/csvw/dataset"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func stats(mdPath string) {
	ds, err := dataset.New(mdPath)
	if err != nil {
		panic(err)
	}
	err = ds.LoadData()
	if err != nil {
		panic(err)
	}

	//fmt.Println(ds.MetadataPath)
	//fmt.Println(":")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
	// noinspection GoUnhandledErrorResultInspection
	fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", "Filename", "Component", "Rows", "FKs")
	// noinspection GoUnhandledErrorResultInspection
	fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", "--------", "---------", "----", "---")
	for _, table := range ds.Tables {
		cname := ""
		if table.Comp != "" {
			cname = table.CanonicalName
		}
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", table.Url, cname, len(table.Data), len(table.ForeignKeys))
	}
	// noinspection GoUnhandledErrorResultInspection
	w.Flush()
}

var statsCmd = &cobra.Command{
	Use:   "stats DATASET",
	Short: "Show summary statistics",
	Long:  "",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		stats(args[0])
	},
}

func init() {
	rootCmd.AddCommand(statsCmd)
}
