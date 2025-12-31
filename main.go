package main

import (
	"gocldf/cmd"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	/*
		bib, err := cldf.NewSources("../../projects/glottolog/glottolog/references/bibtex/hh.bib")
		if err != nil {
			panic(err)
		}
		fmt.Println(len(bib.Items))
		fmt.Println(bib.Items[0].Type, bib.Items[0].Id)
		fmt.Println(bib.SqlCreate())

	*/
	cmd.Execute()
	return
}
