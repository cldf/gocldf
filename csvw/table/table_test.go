package table

import (
	"encoding/json"
	"os"
	"testing"
)

func makeTable(fname string) Table {
	data, err := os.ReadFile("testdata/" + fname)
	if err != nil {
		panic(err)
	}
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		panic(err)
	}
	tbl, err := New(result)
	if err != nil {
		panic(err)
	}
	return *tbl
}

func TestTable_simple(t *testing.T) {
	tbl := makeTable("table_simple.json")
	if len(tbl.PrimaryKey) > 0 {
		t.Errorf(`problem: %q vs %q`, len(tbl.PrimaryKey), 0)
	}
	if len(tbl.Columns) != 1 {
		t.Errorf(`problem: %q vs %q`, len(tbl.Columns), 1)
	}
	if tbl.CanonicalName != "table_simple.csv" {
		t.Errorf(`problem: %q vs %q`, len(tbl.CanonicalName), "table_simple.csv")
	}
	result := make(chan TableRead, 1)
	go tbl.Read("testdata/", result)
	tableRead := <-result
	if tableRead.Err != nil || tableRead.Url != "table_simple.csv" {
		t.Errorf(`problem`)
	}
	if len(tbl.Data) != 3 {
		t.Errorf(`problem`)
	}
	if tbl.Data[0]["ID"] != "a" {
		t.Errorf(`problem`)
	}
}
