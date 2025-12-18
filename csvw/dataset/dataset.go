package dataset

import (
	"encoding/json"
	"gocldf/csvw/table"
	"os"
	"path/filepath"
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
		res.Tables[tbl.CanonicalName()] = tbl
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
