package dbutil

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func Test_WithDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	WithDatabase(dbPath, func(db *sql.DB) error {
		WithTransaction(db, func(tx *sql.Tx) error {
			tx.Exec("CREATE TABLE foo (id INTEGER PRIMARY KEY)")
			BatchInsert(tx, "foo", []string{"id"}, [][]any{[]any{1}, []any{2}})
			return nil
		})
		return nil
	}, false, false)
	var id int
	QueryDatabase(dbPath, "select id from foo", func(rows *sql.Rows) error {
		rows.Scan(&id)
		return nil
	})
	if id != 2 {
		t.Errorf("id != 2")
	}
}
