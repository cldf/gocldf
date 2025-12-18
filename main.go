package main

import (
	"fmt"
	"gocldf/csvw/dataset"
	"os"
	"strconv"
)

func main() {
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
