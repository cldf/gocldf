package main

import (
	"gocldf/cmd"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	cmd.Execute()
	return
}
