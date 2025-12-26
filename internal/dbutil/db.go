package dbutil

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"gocldf/internal/pathutil"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/mod/semver"
)

func WithDatabase(dbPath string, fn func(*sql.DB) error, mustExist bool, foreignKeysOn bool) (err error) {
	if dbPath != ":memory:" && !pathutil.PathExists(dbPath) && mustExist {
		return errors.New("database does not exist")
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer func(db *sql.DB) {
		err = db.Close()
	}(db)
	_, err = db.Exec("PRAGMA journal_mode = MEMORY;")
	if err != nil {
		return err
	}
	_, err = db.Exec("PRAGMA synchronous = OFF;")
	if err != nil {
		return err
	}
	if foreignKeysOn {
		_, err = db.Exec("PRAGMA foreign_keys = ON;")
		if err != nil {
			return err
		}
	}
	err = fn(db)
	if err != nil {
		return err
	}
	return err
}

func WithTransaction(db *sql.DB, fn func(tx *sql.Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback() // Safely ignored if tx.Commit() is called first

	err = fn(tx)
	if err != nil {
		return
	}
	if err = tx.Commit(); err != nil {
		return
	}
	return
}

func BatchInsert(tx *sql.Tx, tableName string, colNames []string, rows [][]any) {
	var (
		version   string
		maxParams = 900
	)
	err := tx.QueryRow("SELECT sqlite_version()").Scan(&version)
	if err == nil {
		if semver.Compare("v"+version, "v3.32.0") >= 0 {
			maxParams = 32000
		}
	}

	batchSize := maxParams / (len(colNames) + 1)

	nCols := len(colNames)
	var insertSql []string
	insertSql = append(insertSql, fmt.Sprintf("INSERT INTO `%v` (", tableName))
	for i, col := range colNames {
		if i > 0 {
			insertSql = append(insertSql, ",")
		}
		insertSql = append(insertSql, fmt.Sprintf("`%v`", col))
	}
	insertSql = append(insertSql, ") VALUES ")
	insert := strings.Join(insertSql, "")

	current := 0
	for current < len(rows) {
		nRows := batchSize
		if nRows+current > len(rows) {
			nRows = len(rows) - current
		}

		rowPlaceholder := "(" + strings.Trim(strings.Repeat("?,", nCols), ",") + ")"
		allPlaceholders := strings.Repeat(rowPlaceholder+",", nRows)
		allPlaceholders = strings.TrimSuffix(allPlaceholders, ",")

		args := make([]any, 0, nRows*nCols)
		for i := 0; i < nRows; i++ {
			args = append(args, rows[current+i]...)
		}
		_, err := tx.Exec(insert+allPlaceholders+";", args...)
		if err != nil {
			log.Fatal(err)
		}

		current += nRows
	}
}

func Query(db *sql.DB, query string, scanner func(*sql.Rows) error, args ...interface{}) (err error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
	}(rows)

	for rows.Next() {
		if err = scanner(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// QueryDatabase is a convenient wrapper around WithDatabase and Query.
func QueryDatabase(dbPath string, query string, scanner func(*sql.Rows) error, args ...interface{}) error {
	err := WithDatabase(dbPath, func(database *sql.DB) error {
		return Query(database, query, scanner, args...)
	}, true, true)
	return err
}
