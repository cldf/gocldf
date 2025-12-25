package cmd

import (
	"encoding/json"
	"fmt"
	"gocldf/csvw"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func stats(out io.Writer, mdPath string, withMetadata bool) error {
	ds, err := csvw.GetLoadedDataset(mdPath)
	if err != nil {
		return err
	}

	fmt.Fprintln(out, ds.MetadataPath+"\n")
	if withMetadata {
		for key, val := range ds.Metadata {
			s, ok := val.(string)
			if !ok {
				res, err := json.Marshal(val)
				if err != nil {
					return err
				}
				s = string(res)
			}
			fmt.Fprint(out, key+":\t")
			fmt.Fprintln(out, s)
			fmt.Fprintln(out, "")
		}
	}
	w := tabwriter.NewWriter(out, 0, 0, 1, ' ', tabwriter.Debug)
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
	return nil
}

var withMetadata bool
var statsCmd = &cobra.Command{
	Use:   "stats DATASET",
	Short: "Show summary statistics",
	Long:  "",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, _ := cmd.Flags().GetString("basepath")
		return stats(cmd.OutOrStderr(), cfg+args[0], withMetadata)
	},
}

func init() {
	statsCmd.Flags().BoolVarP(&withMetadata, "metadata", "m", false, "Also print metadata")
	rootCmd.AddCommand(statsCmd)
}
