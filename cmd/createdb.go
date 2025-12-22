package cmd

import (
	"database/sql"
	"fmt"
	"gocldf/csvw/dataset"
	"gocldf/db"
	"io"

	"github.com/spf13/cobra"
)

func createdb(out io.Writer, mdPath string, dbPath string) error {
	ds, err := dataset.GetLoadedDataset(mdPath)
	if err != nil {
		return err
	}
	err = db.WithDatabase(dbPath, func(database *sql.DB) error {
		return db.WithTransaction(database, func(tx *sql.Tx) error {
			schema, tableData, err := ds.ToSqlite(tx)
			if err != nil {
				return err
			}
			_, err = tx.Exec(schema) // Write the schema ...
			if err != nil {
				return err
			}
			for _, tData := range tableData { // ... and the data.
				db.BatchInsert(tx, tData.TableName, tData.ColNames, tData.Rows)
			}
			return nil
		})
	}, true)
	if err != nil {
		return err
	}
	var count string
	err = db.QueryDatabase(
		dbPath,
		"select cldf_id from languagetable limit 1",
		func(rows *sql.Rows) error {
			return rows.Scan(&count)
		},
	)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, count)
	return nil
}

// var withMetadata bool
var createdbCmd = &cobra.Command{
	Use:   "createdb DATASET DB_PATH",
	Short: "Load CLDF dataset into a SQLite database",
	Long:  "",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		return createdb(cmd.OutOrStdout(), args[0], args[1])
	},
}

func init() {
	//createdbCmd.Flags().BoolVarP(&withMetadata, "metadata", "m", false, "Also print metadata")
	rootCmd.AddCommand(createdbCmd)
}
