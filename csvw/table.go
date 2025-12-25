package csvw

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"gocldf/internal/pathutil"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
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
	Columns       []*Column
	Data          []map[string]interface{}
	ForeignKeys   []*ForeignKey
	Dialect       *Dialect
}

func NewTable(jsonTable map[string]interface{}) (*Table, error) {
	jsonCols := jsonTable["tableSchema"].(map[string]interface{})["columns"].([]interface{})
	columns := make([]*Column, len(jsonCols))
	for i, jsonCol := range jsonCols {
		col, err := NewColumn(i, jsonCol.(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		columns[i] = col
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
			err := json.Unmarshal(js, &fk)
			if err != nil {
				return nil, err
			}
			if len(fk.ColumnReference) == 1 && listValued[fk.ColumnReference[0]] {
				fk.ManyToMany = true
			} else {
				fk.ManyToMany = false
			}
			fks[i] = &fk
		}
	}
	var pk []string
	jsonPk, ok := jsonTable["tableSchema"].(map[string]interface{})["primaryKey"]
	if ok {
		for _, col := range jsonPk.([]interface{}) {
			val, ok := col.(string)
			if ok {
				pk = append(pk, val)
			}
		}
	}
	var (
		dialect *Dialect
		err     error
	)
	_, ok = jsonTable["dialect"]
	if ok {
		dialect, err = NewDialect(jsonTable)
		if err != nil {
			return nil, err
		}
	} else {
		dialect = nil
	}
	res := &Table{
		Url:         jsonTable["url"].(string),
		Columns:     columns,
		Data:        []map[string]interface{}{},
		ForeignKeys: fks,
		PrimaryKey:  pk,
		Dialect:     dialect,
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
	return res, nil
}

// Read a row represented as slice of strings into Go objects.
func (tbl *Table) readRow(fields []string, dialect *Dialect) (map[string]interface{}, error) {
	row := make(map[string]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		if dialect.trim == "true" {
			field = strings.TrimSpace(field)
		} else if dialect.trim == "end" {
			field = strings.TrimRightFunc(field, unicode.IsSpace)
		}
		val, err := tbl.Columns[i].ToGo(field, true)
		if err != nil {
			return nil, err
		}
		row[tbl.Columns[i].CanonicalName] = val
	}
	return row, nil
}

type TableRead struct {
	Url string
	Err error
}

func readZipped(fp string) (bytes []byte, err error) {
	r, err := zip.OpenReader(fp)
	if err != nil {
		return nil, err
	}
	defer func(r *zip.ReadCloser) {
		err = r.Close()
	}(r)

	var contentBytes []byte
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		contentBytes, err = io.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		err = rc.Close()
		if err != nil {
			return nil, err
		} // Must close each file reader individually
		break
	}
	return contentBytes, nil
}

func (tbl *Table) Read(dir string, dialect *Dialect, ch chan<- TableRead) {
	var reader *csv.Reader
	fp := filepath.Join(dir, tbl.Url)
	zipped := false
	if !pathutil.PathExists(fp) {
		fp += ".zip"
		zipped = true
	}
	var (
		rows [][]string
		err  error
	)
	if zipped {
		zippedBytes, err := readZipped(fp)
		if err != nil {
			ch <- TableRead{tbl.Url, err}
			return
		}
		reader = csv.NewReader(bytes.NewReader(zippedBytes))
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
		reader = csv.NewReader(file)
	}
	if tbl.Dialect != nil {
		dialect = tbl.Dialect
	}
	dialect.ConfigureCsvReader(reader)
	rows, err = reader.ReadAll()
	if err != nil {
		ch <- TableRead{tbl.Url, err}
		return
	}

	for rowIndex, row := range rows {
		if !dialect.header || (rowIndex > 0) { // FIXME: take headerRowCount and skipRows into account!
			val, err := tbl.readRow(row, dialect)
			if err != nil {
				ch <- TableRead{tbl.Url, err}
				return
			}
			tbl.Data = append(tbl.Data, val)
		}
	}
	ch <- TableRead{tbl.Url, err}
}

func (tbl *Table) NameToCol() map[string]*Column {
	nameToCol := make(map[string]*Column, len(tbl.Columns))
	for _, col := range tbl.Columns {
		nameToCol[col.Name] = col
	}
	return nameToCol
}

func (tbl *Table) SqlCreateAssociationTable(fk ForeignKey, UrlToTable map[string]*Table) string {
	var res []string
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
	return strings.Join(res, "\n")
}

func (tbl *Table) AssociationTableRowsToSql(
	fk *ForeignKey,
	UrlToTable map[string]*Table,
) (rows [][]any, tableName string, colNames []string, err error) {
	stable := tbl.CanonicalName
	spk := tbl.NameToCol()[tbl.PrimaryKey[0]].CanonicalName
	ttable_ := UrlToTable[fk.Reference.Resource]
	ttable := ttable_.CanonicalName
	tpk := ttable_.NameToCol()[ttable_.PrimaryKey[0]].CanonicalName

	colNames = []string{
		fmt.Sprintf("%v_%v", stable, spk),
		fmt.Sprintf("%v_%v", ttable, tpk),
		"context"}

	colName := tbl.NameToCol()[fk.ColumnReference[0]].CanonicalName
	for _, row := range tbl.Data {
		vals, ok := row[colName].([]string)
		if ok {
			if len(vals) > 0 {
				for _, val := range vals {
					row := []any{tbl.PrimaryKey[0], val, colName}
					rows = append(rows, row)
				}
			}
		}
	}
	return rows, stable + "_" + ttable, colNames, nil
}

func (tbl *Table) ManyToMany() []*ForeignKey {
	var manyToMany []*ForeignKey
	for _, fk := range tbl.ForeignKeys {
		if fk.ManyToMany {
			manyToMany = append(manyToMany, fk)
		}
	}
	return manyToMany
}

func (tbl *Table) SqlCreate(UrlToTable map[string]*Table) (string, error) {
	var (
		res        []string
		pk         []string
		manyToMany []string
		clauses    []string
	)
	for _, fk := range tbl.ManyToMany() {
		manyToMany = append(manyToMany, fk.ColumnReference[0])
	}
	nameToCol := tbl.NameToCol()
	pk = append(pk, "PRIMARY KEY(")
	for i, col := range tbl.PrimaryKey {
		if i > 0 {
			pk = append(pk, ",")
		}
		pk = append(pk, fmt.Sprintf("`%v`", nameToCol[col].CanonicalName))
	}
	pk = append(pk, ")")

	for i, col := range tbl.Columns {
		if !slices.Contains(manyToMany, col.Name) {
			clause := ""
			if i == 0 {
				clause += "\t"
			}
			// FIXME: add CHECK constraints!
			clauses = append(clauses, clause+col.sqlCreate())
			//clauses = append(clauses, clause+fmt.Sprintf("`%v`\t%v", col.CanonicalName, col.Datatype.SqlType()))
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
					return "", errors.New(fmt.Sprintf("unknown column: %v '%v' %v", tbl.Url, col, nameToCol))
				}
			}
			clause += ") ON DELETE CASCADE"
			clauses = append(clauses, clause)
		}
	}

	res = append(res, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%v` (", tbl.CanonicalName))
	res = append(res, strings.Join(clauses, ",\n\t"))
	res = append(res, ");")
	return strings.Join(res, "\n"), nil
}

// RowsToSql returns
//   - a slice of slices representing the rows in the table with values formatted
//     for insertion into SQLite.
//   - a slice of column names representing the column names (in order) for the rows.
func (tbl *Table) RowsToSql() (rows [][]any, colNames []string, err error) {
	var manyToMany []string
	for _, fk := range tbl.ManyToMany() {
		manyToMany = append(manyToMany, fk.ColumnReference[0])
	}
	colMap := make(map[string]*Column)
	listValued := make(map[string]string)
	for _, col := range tbl.Columns {
		if !slices.Contains(manyToMany, col.Name) {
			colNames = append(colNames, col.CanonicalName)
			colMap[col.CanonicalName] = col
			if col.Separator != "" {
				listValued[col.CanonicalName] = col.Separator
			}
		}
		// ManyToMany columns are skipped, because these values are turned into rows in association tables.
	}
	// Now we assemble the rows:
	rows = make([][]any, len(tbl.Data))
	for i, row := range tbl.Data {
		rows[i] = make([]any, len(colNames))
		for j, col := range colNames {
			sep, ok := listValued[col]
			if ok {
				// List-valued columns are assumed to be of datatype string.
				rows[i][j] = strings.Join(row[col].([]string), sep)
			} else {
				val, err := colMap[col].ToSql(row[col])
				if err != nil {
					return rows, colNames, err
				}
				rows[i][j] = val
			}
		}
	}
	return rows, colNames, nil
}
