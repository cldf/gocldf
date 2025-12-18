package table

import (
	"encoding/csv"
	"gocldf/csvw/column"
	"os"
	"path/filepath"
	"strings"
)

type Table struct {
	Url     string
	Comp    string
	Columns []*column.Column
	Data    []map[string]interface{}
}

func New(jsonTable map[string]interface{}) *Table {
	jsonCols := jsonTable["tableSchema"].(map[string]interface{})["columns"].([]interface{})
	columns := make([]*column.Column, len(jsonCols))
	for i, jsonCol := range jsonCols {
		columns[i] = column.New(i, jsonCol.(map[string]interface{}))
	}
	res := &Table{
		Url:     jsonTable["url"].(string),
		Columns: columns,
		Data:    []map[string]interface{}{},
	}
	val, ok := jsonTable["dc:conformsTo"]
	if ok {
		res.Comp = val.(string)
	}
	return res
}

func (tbl *Table) CanonicalName() string {
	if tbl.Comp != "" {
		parts := strings.Split(tbl.Comp, "#")
		return parts[len(parts)-1]
	}
	return tbl.Url
}

func (tbl *Table) ReadRow(fields []string) map[string]interface{} {
	row := make(map[string]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		row[tbl.Columns[i].CanonicalName()] = tbl.Columns[i].ToGo(fields[i], true)
	}
	return row
}

type TableRead struct {
	Url string
	Err error
}

func (tbl *Table) Read(dir string, ch chan<- TableRead) {
	file, err := os.Open(filepath.Join(dir, tbl.Url))
	if err != nil {
		ch <- TableRead{tbl.Url, err}
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			ch <- TableRead{tbl.Url, err}
			return
		}
	}(file)

	rows, err := csv.NewReader(file).ReadAll() // returns [][]string
	if err != nil {
		ch <- TableRead{tbl.Url, err}
		return
	}
	for rowIndex, row := range rows {
		if rowIndex > 0 {
			tbl.Data = append(tbl.Data, tbl.ReadRow(row))
		}
	}
	ch <- TableRead{tbl.Url, err}
}
