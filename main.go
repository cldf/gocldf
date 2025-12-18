package main

import (
	"flag"
	"fmt"
	"gocldf/csvw/dataset"
	"os"
	"strconv"
)

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <CLDF metadata file>\n", os.Args[0])
	}
	if len(os.Args) < 2 {
		flag.Usage()
		return
	}
	ds := dataset.New(os.Args[1:][0])
	ds.LoadData()

	fmt.Println(ds.MetadataPath)
	fmt.Println(":")
	for _, table := range ds.Tables {
		fmt.Println(table.CanonicalName() + ": " + strconv.Itoa(len(table.Columns)) + " columns")
		for _, col := range table.Columns {
			fmt.Println("   " + col.CanonicalName())
		}
		fmt.Println(strconv.Itoa(len(table.Data)) + " rows")
		fmt.Println("ID of first item: ")
		fmt.Println(table.Data[0]["cldf_id"])
		fmt.Println("---")
	}
}
