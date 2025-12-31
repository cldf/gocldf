package cmd

import (
	"bytes"
	"database/sql"
	"gocldf/internal/dbutil"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreatedb(t *testing.T) {
	dir := t.TempDir()
	actual := new(bytes.Buffer)
	rootCmd.SetOut(actual)
	rootCmd.SetErr(actual)
	rootCmd.SetArgs([]string{"createdb", "../cldf/testdata/StructureDataset-metadata.json", filepath.Join(dir, "test.sqlite")})
	rootCmd.Execute()

	expected := `Loaded`
	if !strings.Contains(actual.String(), expected) {
		t.Errorf(`problem: "%q"" not in "%q""`, expected, actual.String())
	}
	var countRefs int
	err := dbutil.QueryDatabase(
		filepath.Join(dir, "test.sqlite"),
		"SELECT count(*) FROM ValueTable as v, ValueTable_SourceTable as vs, SourceTable as s WHERE s.doi = ? AND v.cldf_id = vs.ValueTable_cldf_id AND vs.SourceTable_id = s.id;",
		func(rows *sql.Rows) error {
			err := rows.Scan(&countRefs)
			if err != nil {
				return err
			}
			return nil
		}, "10.1515/jsall-2017-0008")
	if err != nil {
		panic(err)
	}
	if countRefs != 812 {
		t.Errorf(`problem: %v vs. %v`, countRefs, 3)
	}
}
