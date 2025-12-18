package table

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"errors"
	"gocldf/csvw/column"
	"io"
	"log"
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

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true // File or directory exists
	}
	if errors.Is(err, os.ErrNotExist) {
		return false // Specifically does not exist
	}
	// Any other error means we can't confirm existence (e.g., permission denied)
	return false
}

func readZipped(fp string) []byte {
	r, err := zip.OpenReader(fp)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	var contentBytes []byte
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			log.Fatal(err)
		}
		// 4. Read or process file content (e.g., copy to Stdout)
		contentBytes, err = io.ReadAll(rc)
		if err != nil {
			log.Fatal(err)
		}
		rc.Close() // Must close each file reader individually
		break
	}
	return contentBytes
}

func (tbl *Table) Read(dir string, ch chan<- TableRead) {
	fp := filepath.Join(dir, tbl.Url)
	zipped := false
	if !exists(fp) {
		fp += ".zip"
		zipped = true
	}
	var rows [][]string
	var err error
	if zipped {
		rows, err = csv.NewReader(bytes.NewReader(readZipped(fp))).ReadAll()
	} else {
		file, err := os.Open(fp)
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
		rows, err = csv.NewReader(file).ReadAll() // returns [][]string
	}

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
