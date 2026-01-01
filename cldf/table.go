package cldf

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"gocldf/internal/jsonutil"
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
	trimmer       func(string) string
}

func NewTable(jsonTable map[string]interface{}, withSourceTable bool) (tbl *Table, err error) {
	var (
		dialect *Dialect
		trimmer = func(s string) string { return s }
		fks     []*ForeignKey
	)
	tableSchema := jsonTable["tableSchema"].(map[string]interface{})

	jsonCols := tableSchema["columns"].([]interface{})
	columns := make([]*Column, len(jsonCols))
	for i, jsonCol := range jsonCols {
		col, err := NewColumn(i, jsonCol.(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		if withSourceTable && col.CanonicalName == "cldf_source" {
			// remember and store additional foreign key constraint!
			fks = append(
				fks,
				&ForeignKey{
					ManyToMany:      true,
					ColumnReference: []string{"cldf_source"},
					Reference:       Reference{ColumnReference: []string{"id"}, Resource: "SourceTable"}})
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
	jsonFks, ok := tableSchema["foreignKeys"]
	if ok {
		//fks = make([]*ForeignKey, len(jsonFks.([]interface{})))
		for _, jsonFk := range jsonFks.([]interface{}) {
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
			fks = append(fks, &fk)
		}
	}
	pk, err := jsonutil.GetStringArray(tableSchema, "primaryKey")
	if err != nil {
		return nil, err
	}
	_, ok = jsonTable["dialect"]
	if ok {
		dialect, err = NewDialect(jsonTable)
		if err != nil {
			return nil, err
		}
	} else {
		dialect = nil
	}
	if dialect != nil {
		if dialect.trim == "true" {
			trimmer = strings.TrimSpace
		} else if dialect.trim == "end" {
			trimmer = func(s string) string {
				return strings.TrimRightFunc(s, unicode.IsSpace)
			}
		}
	}
	res := &Table{
		Url:         jsonTable["url"].(string),
		Columns:     columns,
		Data:        []map[string]interface{}{},
		ForeignKeys: fks,
		PrimaryKey:  pk,
		Dialect:     dialect,
		trimmer:     trimmer,
	}
	res.Comp, err = jsonutil.GetString(jsonTable, "dc:conformsTo", "")
	if err != nil {
		return nil, err
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
func (tbl *Table) readRow(fields []string, noChecks bool) (map[string]interface{}, error) {
	row := make(map[string]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		val, err := tbl.Columns[i].ToGo(tbl.trimmer(fields[i]), true, noChecks)
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

func (tbl *Table) Read(dir string, dialect *Dialect, noChecks bool, ch chan<- TableRead) {
	var reader *csv.Reader
	fp := filepath.Join(dir, tbl.Url)
	var (
		rows [][]string
		err  error
	)
	r, err := pathutil.Reader(fp)
	if err != nil {
		ch <- TableRead{tbl.Url, err}
		return
	}
	defer func(file any) {
		switch file.(type) {
		case *os.File:
			err = file.(*os.File).Close()
		}
	}(r)
	reader = csv.NewReader(r.(io.Reader))

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
			val, err := tbl.readRow(row, noChecks)
			if err != nil {
				ch <- TableRead{tbl.Url, err}
				return
			}
			tbl.Data = append(tbl.Data, val)
		}
	}
	ch <- TableRead{tbl.Url, err}
}

func (tbl *Table) nameToCol() map[string]*Column {
	nameToCol := make(map[string]*Column, len(tbl.Columns))
	for _, col := range tbl.Columns {
		nameToCol[col.Name] = col
	}
	return nameToCol
}

func (tbl *Table) sqlCreateAssociationTable(fk ForeignKey, UrlToTable map[string]*Table) string {
	var (
		res    []string
		ttable string
		tpk    string
	)
	stable := tbl.CanonicalName
	spk := tbl.nameToCol()[tbl.PrimaryKey[0]].CanonicalName

	if fk.Reference.Resource == "SourceTable" {
		ttable = "SourceTable"
		tpk = "id"
	} else {
		ttable_, ok := UrlToTable[fk.Reference.Resource]
		if !ok {
			panic("not found " + fk.Reference.Resource)
		}
		ttable = ttable_.CanonicalName
		tpk = ttable_.nameToCol()[ttable_.PrimaryKey[0]].CanonicalName
	}
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

func (tbl *Table) associationTableRowsToSql(
	fk *ForeignKey,
	UrlToTable map[string]*Table,
) (rows [][]any, tableName string, colNames []string, err error) {
	var (
		ttable  string
		tpk     string
		colName string
	)
	stable := tbl.CanonicalName
	spk := tbl.nameToCol()[tbl.PrimaryKey[0]].CanonicalName

	if fk.Reference.Resource == "SourceTable" {
		ttable = "SourceTable"
		tpk = "id"
		colName = "cldf_source"
	} else {
		ttable_, ok := UrlToTable[fk.Reference.Resource]
		if !ok {
			panic("not found " + fk.Reference.Resource)
		}
		ttable = ttable_.CanonicalName
		tpk = ttable_.nameToCol()[ttable_.PrimaryKey[0]].CanonicalName
		colName = tbl.nameToCol()[fk.ColumnReference[0]].CanonicalName
	}

	colNames = []string{
		fmt.Sprintf("%v_%v", stable, spk),
		fmt.Sprintf("%v_%v", ttable, tpk),
		"context"}

	for _, row := range tbl.Data {
		vals, ok := row[colName].([]string)
		if ok {
			if len(vals) > 0 {
				for _, val := range vals {
					var (
						context, pages string
						found          bool
					)
					if ttable == "SourceTable" {
						val, pages, found = strings.Cut(val, "[")
						if found {
							if strings.HasSuffix(pages, "]") {
								context = pages[:len(pages)-1]
							} else {
								return rows, stable + "_" + ttable, colNames, errors.New("ill-formatted source")
							}
						}
						context = ""
					} else {
						context = colName
					}
					row := []any{row[spk], val, context}
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

func (tbl *Table) sqlCreate(UrlToTable map[string]*Table, noChecks bool) (string, error) {
	var (
		res        []string
		pk         []string
		manyToMany []string
		clauses    []string
	)
	for _, fk := range tbl.ManyToMany() {
		manyToMany = append(manyToMany, fk.ColumnReference[0])
	}
	nameToCol := tbl.nameToCol()
	if len(tbl.PrimaryKey) > 0 {
		pk = append(pk, "PRIMARY KEY(")
		for i, col := range tbl.PrimaryKey {
			if i > 0 {
				pk = append(pk, ",")
			}
			pk = append(pk, fmt.Sprintf("`%v`", nameToCol[col].CanonicalName))
		}
		pk = append(pk, ")")
	}
	for i, col := range tbl.Columns {
		if !slices.Contains(manyToMany, col.Name) {
			clause := ""
			if i == 0 {
				clause += "\t"
			}
			clauses = append(clauses, clause+col.sqlCreate(noChecks))
		}
	}
	if len(pk) > 0 {
		clauses = append(clauses, strings.Join(pk, ""))
	}

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
				val, ok := ttable.nameToCol()[col]
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

// rowsToSql returns
//   - a slice of slices representing the rows in the table with values formatted
//     for insertion into SQLite.
//   - a slice of column names representing the column names (in order) for the rows.
func (tbl *Table) rowsToSql() (rows [][]any, colNames []string, err error) {
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
