package main

import (
	"database/sql"
	"flag"
	"fmt"
	"gocldf/cmd"
	"gocldf/csvw/dataset"
	"gocldf/db"
	"os"
	"text/tabwriter"

	_ "github.com/mattn/go-sqlite3"
)

/*
	Type                  Rows

-----------------  -----------------  -------
media.csv          MediaTable             583
examples.csv       ExampleTable         94672
languages.csv      LanguageTable           42
contributions.csv  ContributionTable       42
parameters.csv     ParameterTable         335
speakers.csv                              289
phones.csv                            1863702
words.csv                              896664
glosses.csv                              2053
sources.bib        Sources                 52
*/
func stats(ds *dataset.Dataset) {
	err := ds.LoadData()
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

func createdb(ds *dataset.Dataset, dbPath string) {
	err := ds.LoadData()
	if err != nil {
		panic(err)
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
		panic("Error: " + err.Error())
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
		panic("Error querying database: " + err.Error())
	}
	fmt.Println(count)
}

func main() {
	cmd.Execute()
	return
	flag.Usage = func() {
		fmt.Printf("Usage: %s %s <CLDF metadata file> [*ARGS]\n", "CMD {stats|createdb}", os.Args[0])
	}
	if len(os.Args) < 3 {
		flag.Usage()
		return
	}
	ds, err := dataset.New(os.Args[2])
	if err != nil {
		panic(err)
	}
	switch os.Args[1] {
	case "stats":
		stats(ds)
	case "createdb":
		createdb(ds, os.Args[3])
	default:
		flag.Usage()
	}
}
