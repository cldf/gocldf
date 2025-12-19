package main

import (
	"flag"
	"fmt"
	"gocldf/csvw/dataset"
	"os"
	"text/tabwriter"
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
func stats(ds *dataset.Dataset, db_path string) {
	ds.LoadData()

	//fmt.Println(ds.MetadataPath)
	//fmt.Println(":")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
	fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", "Filename", "Component", "Rows", "FKs")
	fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", "--------", "---------", "----", "---")
	for _, table := range ds.Tables {
		cname := ""
		if table.Comp != "" {
			cname = table.CanonicalName
		}
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", table.Url, cname, len(table.Data), len(table.ForeignKeys))

		//fmt.Println(table.SqlCreate())
		//fmt.Println(table.SqlCreateAssociationTables(ds.UrlToCanonicalName()))
	}
	//fmt.Println(ds.SqlSchema())
	ds.ToSqlite(db_path)
	w.Flush()
}

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <CLDF metadata file>\n", os.Args[0])
	}
	if len(os.Args) < 2 {
		flag.Usage()
		return
	}
	ds := dataset.New(os.Args[1:][0])
	stats(ds, os.Args[2])
}
