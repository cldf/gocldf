package table

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"gocldf/csvw/column"
	"gocldf/db"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type Reference struct {
	Resource        string
	ColumnReference []string
}

type ForeignKey struct {
	ManyToMany      bool
	ColumnReference []string
	Reference       Reference
}

type Table struct {
	Url           string
	Comp          string
	CanonicalName string
	PrimaryKey    []string
	Columns       []*column.Column
	Data          []map[string]interface{}
	ForeignKeys   []*ForeignKey
}

func New(jsonTable map[string]interface{}) *Table {
	jsonCols := jsonTable["tableSchema"].(map[string]interface{})["columns"].([]interface{})
	columns := make([]*column.Column, len(jsonCols))
	for i, jsonCol := range jsonCols {
		columns[i] = column.New(i, jsonCol.(map[string]interface{}))
	}
	listValued := make(map[string]bool, len(columns))
	for _, col := range columns {
		if col.Separator != "" {
			listValued[col.Name] = true
		} else {
			listValued[col.Name] = false
		}
	}
	jsonFks, ok := jsonTable["tableSchema"].(map[string]interface{})["foreignKeys"]
	var fks []*ForeignKey
	if ok {
		fks = make([]*ForeignKey, len(jsonFks.([]interface{})))
		for i, jsonFk := range jsonFks.([]interface{}) {
			fk := ForeignKey{}
			js, _ := json.Marshal(jsonFk)
			json.Unmarshal(js, &fk)
			if len(fk.ColumnReference) == 1 && listValued[fk.ColumnReference[0]] {
				fk.ManyToMany = true
			} else {
				fk.ManyToMany = false
			}
			fks[i] = &fk
		}
	}
	pk := []string{}
	for _, col := range jsonTable["tableSchema"].(map[string]interface{})["primaryKey"].([]interface{}) {
		val, ok := col.(string)
		if ok {
			pk = append(pk, val)
		}
	}
	res := &Table{
		Url:         jsonTable["url"].(string),
		Columns:     columns,
		Data:        []map[string]interface{}{},
		ForeignKeys: fks,
		PrimaryKey:  pk,
	}
	val, ok := jsonTable["dc:conformsTo"]
	if ok {
		res.Comp = val.(string)
	}
	if res.Comp != "" {
		parts := strings.Split(res.Comp, "#")
		res.CanonicalName = parts[len(parts)-1]
	} else {
		res.CanonicalName = res.Url
	}
	return res
}

func (tbl *Table) ReadRow(fields []string) map[string]interface{} {
	row := make(map[string]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		row[tbl.Columns[i].CanonicalName] = tbl.Columns[i].ToGo(fields[i], true)
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

func (tbl *Table) NameToCol() map[string]*column.Column {
	nameToCol := make(map[string]*column.Column, len(tbl.Columns))
	for _, col := range tbl.Columns {
		nameToCol[col.Name] = col
	}
	return nameToCol
}

func (tbl *Table) SqlCreateAssociationTables(UrlToTable map[string]*Table) string {
	var res []string
	for _, fk := range tbl.ForeignKeys {
		if fk.ManyToMany {
			stable := tbl.CanonicalName
			spk := tbl.NameToCol()[tbl.PrimaryKey[0]].CanonicalName
			ttable_ := UrlToTable[fk.Reference.Resource]
			ttable := ttable_.CanonicalName
			tpk := ttable_.NameToCol()[ttable_.PrimaryKey[0]].CanonicalName
			res = append(res, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS `%v_%v` (", stable, ttable))
			res = append(res, fmt.Sprintf(
				"\t`%v_%v`\tTEXT,", stable, spk))
			res = append(res, fmt.Sprintf(
				"\t`%v_%v`\tTEXT,", ttable, tpk))
			res = append(res, fmt.Sprintf(
				"\t`%v`\tTEXT,", "context"))
			res = append(res, fmt.Sprintf(
				"\tFOREIGN KEY (`%v_%v`) REFERENCES `%v`(`%v`) ON DELETE CASCADE,",
				stable, spk, stable, spk))
			res = append(res, fmt.Sprintf(
				"\tFOREIGN KEY (`%v_%v`) REFERENCES `%v`(`%v`) ON DELETE CASCADE",
				ttable, tpk, ttable, tpk))
			res = append(res, ");")
		}
	}
	return strings.Join(res, "\n")
}

func (tbl *Table) SqlInsertAssociationTables(database *sql.DB, UrlToTable map[string]*Table) {
	for _, fk := range tbl.ForeignKeys {
		if fk.ManyToMany {
			stable := tbl.CanonicalName
			spk := tbl.NameToCol()[tbl.PrimaryKey[0]].CanonicalName
			ttable_ := UrlToTable[fk.Reference.Resource]
			ttable := ttable_.CanonicalName
			tpk := ttable_.NameToCol()[ttable_.PrimaryKey[0]].CanonicalName

			var insertSql []string
			insertSql = append(insertSql, fmt.Sprintf("INSERT INTO `%v_%v` (", stable, ttable))
			insertSql = append(insertSql, fmt.Sprintf("`%v_%v`, `%v_%v`, context", stable, spk, ttable, tpk))
			insertSql = append(insertSql, ") VALUES ")

			db.WithTransaction(database, func(tx *sql.Tx) {
				colName := tbl.NameToCol()[fk.ColumnReference[0]].CanonicalName
				for _, row := range tbl.Data {
					vals, ok := row[colName].([]string)
					if ok {
						if len(vals) > 0 {
							rows := make([][]any, len(vals))
							for i, val := range vals {
								rows[i] = make([]any, 3)
								rows[i][0] = tbl.PrimaryKey[0]
								rows[i][1] = val
								rows[i][2] = colName
							}
							db.BatchInsert(tx, strings.Join(insertSql, ""), rows, 3)
						}
					}
				}
			})
		}
	}
}

func (tbl *Table) ManyToManyCols() []string {
	var manyToManyCols []string
	for _, col := range tbl.ForeignKeys {
		if col.ManyToMany {
			manyToManyCols = append(manyToManyCols, col.ColumnReference[0])
		}
	}
	return manyToManyCols
}

func (tbl *Table) SqlCreate(UrlToTable map[string]*Table) string {
	var (
		res []string
		pk  []string
	)
	manyToManyCols := tbl.ManyToManyCols()
	nameToCol := tbl.NameToCol()
	pk = append(pk, "PRIMARY KEY(")
	for i, col := range tbl.PrimaryKey {
		if i > 0 {
			pk = append(pk, ",")
		}
		pk = append(pk, fmt.Sprintf("`%v`", nameToCol[col].CanonicalName))
	}
	pk = append(pk, ")")

	clauses := []string{}
	for i, col := range tbl.Columns {
		if !slices.Contains(manyToManyCols, col.Name) {
			clause := ""
			if i == 0 {
				clause += "\t"
			}
			clauses = append(clauses, clause+fmt.Sprintf("`%v`\t%v", col.CanonicalName, col.Datatype.SqlType()))
		}
	}
	clauses = append(clauses, strings.Join(pk, ""))

	for _, fk := range tbl.ForeignKeys {
		if !fk.ManyToMany {
			clause := "FOREIGN KEY("
			for i, col := range fk.ColumnReference {
				if i > 0 {
					clause += ","
				}
				clause += fmt.Sprintf("`%v`", nameToCol[col].CanonicalName)
			}
			clause += ") REFERENCES "
			ttable := UrlToTable[fk.Reference.Resource]
			clause += fmt.Sprintf("`%v`(", ttable.CanonicalName)
			for i, col := range fk.Reference.ColumnReference {
				if i > 0 {
					clause += ","
				}
				val, ok := ttable.NameToCol()[col]
				if ok {
					clause += fmt.Sprintf("`%v`", val.CanonicalName)
				} else {
					panic(fmt.Sprintf("unknown column: %v '%v' %v", tbl.Url, col, nameToCol))
				}
			}
			clause += ") ON DELETE CASCADE"
			clauses = append(clauses, clause)
		}
	}

	res = append(res, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%v` (", tbl.CanonicalName))
	res = append(res, strings.Join(clauses, ",\n\t"))
	res = append(res, ");")
	return strings.Join(res, "\n")
}

func (tbl *Table) SqlInsert(database *sql.DB) {
	manyToManyCols := tbl.ManyToManyCols()
	var insertSql []string
	var colNames []string
	colMap := make(map[string]*column.Column)
	listValued := make(map[string]string)
	for _, col := range tbl.Columns {
		if !slices.Contains(manyToManyCols, col.Name) {
			colNames = append(colNames, col.CanonicalName)
			colMap[col.CanonicalName] = col
			if col.Separator != "" {
				listValued[col.CanonicalName] = col.Separator
			}
		}
	}
	insertSql = append(insertSql, fmt.Sprintf("INSERT INTO `%v` (", tbl.CanonicalName))
	for i, col := range colNames {
		if i > 0 {
			insertSql = append(insertSql, ",")
		}
		insertSql = append(insertSql, fmt.Sprintf("`%v`", col))
	}
	insertSql = append(insertSql, ") VALUES ")
	rows := make([][]any, len(tbl.Data))
	for i, row := range tbl.Data {
		rows[i] = make([]any, len(colNames))
		for j, col := range colNames {
			sep, ok := listValued[col]
			if ok {
				rows[i][j] = strings.Join(row[col].([]string), sep)
			} else {
				rows[i][j] = colMap[col].Datatype.ToSql(row[col])
			}
		}
	}
	db.WithTransaction(database, func(tx *sql.Tx) {
		db.BatchInsert(tx, strings.Join(insertSql, ""), rows, len(colNames))
	})
}
