package main

// DB_CONNECT="user=pkelly host=localhost port=5432 dbname=pkelly sslmode=disable" DB_SCHEMA=public go run genquery.go "select * from blirp"

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	// db connect, schema from env vars
	tx, closer := dbConnectSetup(os.Getenv("DB_CONNECT"), os.Getenv("DB_SCHEMA"))
	defer closer()

	// sql query is first command line argument
	rows, err := tx.Query(os.Args[1])
	FatalIfErr("executing query", err)

	metadata := GetMetadata(rows)
	DumpMetadata(metadata)

	for rows.Next() {
		data := ScanRow(rows, metadata)
		log.Printf("data: %v", data)

		nullName, err := data.GetString("name")
		FatalIfErr("retrieve name", err)
		name, err := nullName.Value()
		FatalIfErr("retrieve name value", err)

		log.Printf("column name has value %s", name)

		nullIsGood, err := data.GetBool("is_good")
		FatalIfErr("retrieve is_good", err)
		isGood, err := nullIsGood.Value()
		FatalIfErr("retrieve is_good value", err)

		log.Printf("column is_good has value %t", isGood)

	}
}

// caller should defer the func returned
func dbConnectSetup(connectURL, schemaName string) (tx *sql.Tx, closer func()) {

	db, err := sql.Open("postgres", connectURL)
	FatalIfErr("opening database "+schemaName, err)

	tx, err = db.Begin()
	FatalIfErr("beginning transaction", err)

	_, err = tx.Exec("set search_path to " + schemaName)
	FatalIfErr("setting search_path", err)

	return tx, func() {
		// err = tx.Rollback()
		// FatalIfErr("rolling back transaction", err)

		err = db.Close()
		FatalIfErr("closing db", err)
	}
}

// FatalIfErr bails out if there is an error.
func FatalIfErr(where string, err error) {
	if err != nil {
		log.Fatalf("failing at %s: %s", where, err)
	}
}
