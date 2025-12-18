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
func stats(ds *dataset.Dataset) {
	ds.LoadData()

	//fmt.Println(ds.MetadataPath)
	//fmt.Println(":")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
	fmt.Fprintf(w, "%v\t%v\t%v\n", "Filename", "Component", "Rows")
	fmt.Fprintf(w, "%v\t%v\t%v\n", "--------", "---------", "----")
	for _, table := range ds.Tables {
		cname := ""
		if table.Comp != "" {
			cname = table.CanonicalName()
		}
		fmt.Fprintf(w, "%v\t%v\t%v\n", table.Url, cname, len(table.Data))
		/*
			fmt.Println(table.CanonicalName() + ": " + strconv.Itoa(len(table.Columns)) + " columns")
			for _, col := range table.Columns {
				fmt.Println("   " + col.CanonicalName())
			}
			fmt.Println(strconv.Itoa(len(table.Data)) + " rows")
			fmt.Println("ID of first item: ")
			fmt.Println(table.Data[0]["cldf_id"])
			fmt.Println("---")

		*/
	}
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
	stats(ds)
}
