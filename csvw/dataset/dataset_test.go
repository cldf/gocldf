package dataset

import (
	"database/sql"
	"gocldf/db"
	"testing"
)

func makeDataset(fname string) *Dataset {
	ds, err := New("testdata/" + fname)
	if err != nil {
		panic(err)
	}
	return ds
}

func TestDataset_simple(t *testing.T) {
	ds := makeDataset("StructureDataset-metadata.json")
	if len(ds.Tables) != 4 {
		t.Errorf(`problem: %q vs %q`, len(ds.Tables), 4)
	}
	err := ds.LoadData()
	if err != nil {
		panic(err)
	}
	db.WithDatabase(":memory:", func(s *sql.DB) error {
		err = db.WithTransaction(s, func(tx *sql.Tx) error {
			schema, tableData, err := ds.ToSqlite(tx)
			if err != nil {
				return err
			}
			_, err = tx.Exec(schema) // Write the schema ...
			if err != nil {
				return err
			}
			for _, tData := range tableData { // ... and the data.
				db.BatchInsert(tx, tData.TableName, tData.ColNames, tData.Rows)
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		var countLanguages int
		err = db.Query(
			s,
			"select count(*) from languagetable",
			func(rows *sql.Rows) error {
				return rows.Scan(&countLanguages)
			})
		if err != nil {
			panic(err)
		}
		if countLanguages != 29 {
			t.Errorf(`problem: %q vs %q`, countLanguages, 29)
		}
		return nil
	}, false)
}
