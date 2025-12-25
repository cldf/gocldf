package csvw

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func makeDialect() *Dialect {
	res, err := NewDialect(map[string]any{"key": 1})
	if err != nil {
		panic(err)
	}
	return res
}

func makeTable(fname string, load bool) Table {
	data, err := os.ReadFile("testdata/" + fname)
	if err != nil {
		panic(err)
	}
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		panic(err)
	}
	tbl, err := NewTable(result)
	if err != nil {
		panic(err)
	}
	if load {
		result := make(chan TableRead, 1)
		go tbl.Read("testdata/", makeDialect(), result)
		_ = <-result
	}
	return *tbl
}

func TestTable_simple(t *testing.T) {
	tbl := makeTable("table_simple.json", false)
	if len(tbl.PrimaryKey) != 1 {
		t.Errorf(`problem: %q vs %q`, len(tbl.PrimaryKey), 0)
	}
	if len(tbl.Columns) != 2 {
		t.Errorf(`problem: %q vs %q`, len(tbl.Columns), 1)
	}
	if tbl.CanonicalName != "table_simple.csv" {
		t.Errorf(`problem: %q vs %q`, len(tbl.CanonicalName), "table_simple.csv")
	}
	result := make(chan TableRead, 1)
	go tbl.Read("testdata/", makeDialect(), result)
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
	if tbl.Data[0]["Separated"].([]string)[1] != "v" {
		t.Errorf(`problem`)
	}
	if len(tbl.Data[1]["Separated"].([]string)) != 0 {
		t.Errorf(`problem`)
	}
	if tbl.Data[2]["Separated"].([]string)[0] != "z" {
		t.Errorf(`problem`)
	}
}

func TestTable_with_dialect(t *testing.T) {
	tbl := makeTable("table_with_dialect.json", true)
	if tbl.Dialect == nil {
		t.Errorf(`problem: expected dialect`)
	}
	if len(tbl.Data) != 2 {
		t.Errorf(`problem: expected %q rows got %q`, 2, len(tbl.Data))
	}
	if len(tbl.Data[0]) != 3 {
		t.Errorf(`problem: expected %q columns got %q`, 3, len(tbl.Data[0]))
	}
	if tbl.Data[1]["Col"].(string) != "y " {
		t.Errorf(`problem: expected "y " columns got %q`, tbl.Data[1]["Col"].(string))
	}
}

func TestTable_with_dialect2(t *testing.T) {
	tbl := makeTable("table_with_dialect2.json", true)
	if tbl.Data[1]["Col"].(string) != "y" {
		t.Errorf(`problem: expected "y " columns got %q`, tbl.Data[1]["Col"].(string))
	}
}

func TestTable_complex(t *testing.T) {
	tbl := makeTable("table_complex.json", false)
	if len(tbl.PrimaryKey) != 1 {
		t.Errorf(`problem: %q vs %q`, len(tbl.PrimaryKey), 0)
	}
	if tbl.CanonicalName != "MediaTable" {
		t.Errorf(`problem: %q vs %q`, len(tbl.CanonicalName), "MediaTable")
	}
}

func TestTable_fk(t *testing.T) {
	tbl := makeTable("table_complex.json", true)
	if len(tbl.ForeignKeys) != 2 {
		t.Errorf(`problem`)
	}
	if len(tbl.ManyToMany()) != 1 {
		t.Errorf(`problem`)
	}
	tbl2 := makeTable("table_simple.json", true)
	urlToTable := map[string]*Table{"table_simple.csv": &tbl2}
	sql, _ := tbl.SqlCreate(urlToTable)
	if !strings.Contains(sql, "PRIMARY KEY") {
		t.Errorf(`problem`)
	}
	data, colNames, _ := tbl.RowsToSql()
	if len(data) != 3 {
		t.Errorf(`problem: %v vs %v`, len(data), 3)
	}
	sql = tbl.SqlCreateAssociationTable(*tbl.ManyToMany()[0], urlToTable)
	if !strings.Contains(sql, "context") {
		t.Errorf(`problem`)
	}
	data, tName, colNames, _ := tbl.AssociationTableRowsToSql(tbl.ManyToMany()[0], urlToTable)
	if len(data) != 3 {
		t.Errorf(`problem: %v vs %v`, len(data), 3)
	}
	if tName != "MediaTable_table_simple.csv" {
		t.Errorf(`problem`)
	}
	if colNames[1] != "table_simple.csv_ID" {
		t.Errorf(`problem`)
	}
}
