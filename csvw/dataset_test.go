package csvw

import (
	"database/sql"
	"gocldf/internal/dbutil"
	"testing"
)

func makeDataset(fname string) *Dataset {
	ds, err := NewDataset("testdata/" + fname)
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
	dbutil.WithDatabase(":memory:", func(s *sql.DB) error {
		err = dbutil.WithTransaction(s, func(tx *sql.Tx) error {
			schema, tableData, err := ds.ToSqlite()
			if err != nil {
				return err
			}
			_, err = tx.Exec(schema) // Write the schema ...
			if err != nil {
				return err
			}
			for _, tData := range tableData { // ... and the data.
				dbutil.BatchInsert(tx, tData.TableName, tData.ColNames, tData.Rows)
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		var countLanguages int
		err = dbutil.Query(
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
