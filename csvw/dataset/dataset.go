package dataset

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"gocldf/csvw/table"
	"os"
	"path/filepath"
	"slices"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type Dataset struct {
	MetadataPath string
	Metadata     map[string]interface{}
	Tables       map[string]*table.Table
}

func New(md_path string) *Dataset {
	data, err := os.ReadFile(md_path)
	if err != nil {
		panic(err)
	}
	var result map[string]interface{}

	err = json.Unmarshal(data, &result)
	if err != nil {
		panic(err)
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
		tbl := table.New(value.(map[string]interface{}))
		res.Tables[tbl.CanonicalName] = tbl
	}
	return &res
}

func (dataset *Dataset) LoadData() {
	results := make(chan table.TableRead, len(dataset.Tables))
	for _, tbl := range dataset.Tables {
		go tbl.Read(filepath.Dir(dataset.MetadataPath), results)
	}
	for i := 0; i < len(dataset.Tables); i++ {
		tableRead := <-results
		if tableRead.Err != nil {
			panic(tableRead.Err)
		}
	}
	close(results)
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

func (dataset *Dataset) SqlSchema() string {
	var (
		res           []string
		orderedTables = dataset.OrderedTables()
		urlToName     = dataset.UrlToCanonicalName()
		urlToTable    = dataset.UrlToTable()
	)
	for _, url := range orderedTables {
		tbl, ok := dataset.Tables[urlToName[url]]
		if ok {
			res = append(res, tbl.SqlCreate(urlToTable))
		}
	}
	for _, url := range orderedTables {
		tbl, ok := dataset.Tables[urlToName[url]]
		if ok {
			res = append(res, tbl.SqlCreateAssociationTables(urlToTable))
		}
	}
	return strings.Join(res, "\n")
}

func (dataset *Dataset) OrderedTables() []string {
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
			panic("there may be cyclic dependencies between tables")
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
				panic("table not found")
			}
		}
		if delIndex >= 0 {
			tables = slices.Delete(tables, delIndex, delIndex+1)
		}
	}
	return orderedTables
}

func (dataset *Dataset) ToSqlite(db_path string) {
	os.Create(db_path)

	db, err := sql.Open("sqlite3", db_path)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	db.Exec("PRAGMA journal_mode = WAL;")
	db.Exec("PRAGMA synchronous = NORMAL;")

	_, err = db.Exec(dataset.SqlSchema())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	orderedTables := dataset.OrderedTables()
	urlToName := dataset.UrlToCanonicalName()
	urlToTable := dataset.UrlToTable()

	for _, url := range orderedTables {
		tbl, ok := dataset.Tables[urlToName[url]]
		if ok {
			tbl.SqlInsert(db)
		}
	}

	for _, url := range orderedTables {
		tbl, ok := dataset.Tables[urlToName[url]]
		if ok {
			tbl.SqlInsertAssociationTables(db, urlToTable)
		}
	}

	//stmt, err := db.Prepare("SELECT * FROM album WHERE id = ?")
	db.Close()
}
