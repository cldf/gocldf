package db

import (
	"database/sql"
	"log"
	"strings"
)

// FIXME: implement func WithDatabase(dbPath, fn func(database *sql.DB)) (err error) {}

func WithTransaction(db *sql.DB, fn func(tx *sql.Tx)) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	fn(tx)
	return err
}

func BatchInsert(db *sql.Tx, insertSql string, rows [][]any, nCols int) {
	batchSize := 500
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
		_, err := db.Exec(insertSql+allPlaceholders+";", args...)
		if err != nil {
			log.Fatal(err)
		}

		current += nRows
	}
}
