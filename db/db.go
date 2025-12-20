package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"golang.org/x/mod/semver"
)

func pathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true // Path exists
	}
	if errors.Is(err, os.ErrNotExist) {
		return false // Specifically does not exist
	}
	return false
}

func WithDatabase(dbPath string, fn func(*sql.DB) error, recreate bool) error {
	if !pathExists(dbPath) || recreate {
		create, err := os.Create(dbPath)
		if err != nil {
			return err
		}
		defer func(create *os.File) {
			err := create.Close()
			if err != nil {
				panic(err)
			}
		}(create)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	_, err = db.Exec("PRAGMA journal_mode = WAL;")
	if err != nil {
		return err
	}
	_, err = db.Exec("PRAGMA synchronous = NORMAL;")
	if err != nil {
		return err
	}
	err = fn(db)
	err = db.Close()
	if err != nil {
		return err
	}
	return err
}

func WithTransaction(db *sql.DB, fn func(tx *sql.Tx)) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fn(tx)
	if err = tx.Commit(); err != nil {
		return err
	}
	return err
}

func BatchInsert(db *sql.Tx, tableName string, colNames []string, rows [][]any) {
	var (
		version   string
		maxParams int = 900
	)
	err := db.QueryRow("SELECT sqlite_version()").Scan(&version)
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
		_, err := db.Exec(insert+allPlaceholders+";", args...)
		if err != nil {
			log.Fatal(err)
		}

		current += nRows
	}
}
