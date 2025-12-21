package dataset

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"gocldf/csvw/table"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type Dataset struct {
	MetadataPath string
	Metadata     map[string]interface{}
	Tables       map[string]*table.Table
}

func New(md_path string) (*Dataset, error) {
	data, err := os.ReadFile(md_path)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}

	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]interface{}, len(result)-1)
	for k, v := range result {
		if k == "tables" {
			continue
		}
		metadata[k] = v
	}

	res := Dataset{
		md_path,
		metadata,
		make(map[string]*table.Table)}
	for _, value := range result["tables"].([]interface{}) {
		tbl, err := table.New(value.(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		res.Tables[tbl.CanonicalName] = tbl
	}
	return &res, nil
}

func (dataset *Dataset) LoadData() error {
	results := make(chan table.TableRead, len(dataset.Tables))
	for _, tbl := range dataset.Tables {
		go tbl.Read(filepath.Dir(dataset.MetadataPath), results)
	}
	for i := 0; i < len(dataset.Tables); i++ {
		tableRead := <-results
		if tableRead.Err != nil {
			return tableRead.Err
		}
	}
	close(results)
	return nil
}

func (dataset *Dataset) UrlToTable() map[string]*table.Table {
	res := map[string]*table.Table{}
	for _, tbl := range dataset.Tables {
		res[tbl.Url] = tbl
	}
	return res
}

func (dataset *Dataset) UrlToCanonicalName() map[string]string {
	res := map[string]string{}
	for _, tbl := range dataset.Tables {
		res[tbl.Url] = tbl.CanonicalName
	}
	return res
}

func (dataset *Dataset) orderedTables() (map[string]*table.Table, error) {
	var urlToName = dataset.UrlToCanonicalName()
	// Determine the order in which to create the tables
	tables := []string{}
	orderedTables := []string{}
	for _, tbl := range dataset.Tables {
		tables = append(tables, tbl.Url)
	}
	j := 0
	for len(tables) > 0 {
		j++
		if j > 100 {
			return nil, errors.New("there may be cyclic dependencies between tables")
		}
		// We loop over all tables that have not been ordered yet, trying to find one with
		// only fks to already ordered tables.
		delIndex := -1
		for i, url := range tables {
			allRefsInOrdered := true
			val, ok := dataset.Tables[urlToName[url]]
			if ok {
				for _, fk := range val.ForeignKeys {
					if fk.Reference.Resource == url {
						// A self-referential FK. We ignore those anyway.
						continue
					}
					if !slices.Contains(orderedTables, fk.Reference.Resource) {
						allRefsInOrdered = false
					}
				}
				if allRefsInOrdered == true {
					orderedTables = append(orderedTables, url)
					delIndex = i
					break
				}
			} else {
				return nil, errors.New("table not found")
			}
		}
		if delIndex >= 0 {
			tables = slices.Delete(tables, delIndex, delIndex+1)
		}
	}
	orderedTableMap := make(map[string]*table.Table, len(orderedTables))
	for _, url := range orderedTables {
		tbl, ok := dataset.Tables[urlToName[url]]
		if ok {
			orderedTableMap[url] = tbl
		} else {
			return orderedTableMap, fmt.Errorf("table %s not found", url)
		}
	}
	return orderedTableMap, nil
}

func (dataset *Dataset) sqlSchema() (string, error) {
	var (
		res        []string
		urlToTable = dataset.UrlToTable()
	)
	orderedTableMap, err := dataset.orderedTables()
	if err != nil {
		return "", err
	}

	for _, tbl := range orderedTableMap {
		schema, err := tbl.SqlCreate(urlToTable)
		if err != nil {
			return "", err
		} else {
			res = append(res, schema)
		}
	}
	for _, tbl := range orderedTableMap {
		for _, fk := range tbl.ManyToMany() {
			res = append(res, tbl.SqlCreateAssociationTable(*fk, urlToTable))
		}
	}
	return strings.Join(res, "\n"), nil
}

type TableData struct {
	TableName string
	ColNames  []string
	Rows      [][]any
}

// Function ToSqlite returns the data necessary to load the dataset into a SQLite database.
func (dataset *Dataset) ToSqlite(tx *sql.Tx) (string, []TableData, error) {
	var tableData = []TableData{}

	schema, err := dataset.sqlSchema()
	if err != nil {
		return "", tableData, err
	}
	orderedTables, err := dataset.orderedTables()
	if err != nil {
		return "", tableData, err
	}
	urlToTable := dataset.UrlToTable()

	for _, tbl := range orderedTables {
		rows, colNames, err := tbl.RowsToSql()
		if err != nil {
			return "", tableData, err
		}
		tableData = append(tableData, TableData{tbl.CanonicalName, colNames, rows})
	}
	for _, tbl := range orderedTables {
		for _, fk := range tbl.ManyToMany() {
			rows, tableName, colNames, err := tbl.AssociationTableRowsToSql(fk, urlToTable)
			if err != nil {
				return "", tableData, err
			}
			tableData = append(tableData, TableData{tableName, colNames, rows})
		}
	}
	return schema, tableData, nil
}
