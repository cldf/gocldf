package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	"gocldf/csvw/dataset"
	"gocldf/internal/dbutil"
	"gocldf/internal/pathutil"
	"io"
	"os"
	"slices"

	"github.com/spf13/cobra"
)

func createdb(out io.Writer, mdPath string, dbPath string, overwrite bool) error {
	if pathutil.PathExists(dbPath) {
		if overwrite {
			err := os.Remove(dbPath)
			if err != nil {
				return err
			}
		} else {
			return errors.New("database already exists")
		}
	}
	ds, err := dataset.GetLoadedDataset(mdPath)
	if err != nil {
		return err
	}
	err = dbutil.WithDatabase(dbPath, func(database *sql.DB) error {
		return dbutil.WithTransaction(database, func(tx *sql.Tx) error {
			schema, tableData, err := ds.ToSqlite(tx)
			if err != nil {
				return err
			}
			_, err = tx.Exec(schema) // Write the schema ...
			if err != nil {
				return err
			}
			for _, tData := range tableData { // ... and the data.
				dbutil.BatchInsert(tx, tData.TableName, tData.ColNames, tData.Rows)
			}
			return nil
		})
	}, false)
	if err != nil {
		return err
	}
	// We run a query to make sure it worked
	var tableNames []string
	err = dbutil.QueryDatabase(
		dbPath,
		"SELECT name FROM sqlite_master WHERE type='table';",
		func(rows *sql.Rows) error {
			var tableName string
			err := rows.Scan(&tableName)
			if err != nil {
				return err
			}
			tableNames = append(tableNames, tableName)
			return nil
		},
	)
	if err != nil {
		return err
	}
	for _, tbl := range ds.Tables {
		if !slices.Contains(tableNames, tbl.CanonicalName) {
			return fmt.Errorf("table %s not found in database", tbl.CanonicalName)
		}
	}
	fmt.Fprintf(out, "Loaded dataset at\n%v\ninto SQLite database at\n%v\n", mdPath, dbPath)
	return nil
}

var overwrite bool
var createdbCmd = &cobra.Command{
	Use:   "createdb DATASET DB_PATH",
	Short: "Load CLDF dataset into a SQLite database",
	Long:  "",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		return createdb(cmd.OutOrStdout(), args[0], args[1], overwrite)
	},
}

func init() {
	createdbCmd.Flags().BoolVarP(&overwrite, "overwrite", "f", false, "Overwrite SQLite file if exists")
	rootCmd.AddCommand(createdbCmd)
}
