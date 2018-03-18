package main

import (
	"database/sql"
	"log"
)

// Metadata tracks what columns exist and what their data types are.
type Metadata struct {
	ColumnNames []string
	ColumnTypes map[string]string
}

// NewMetadata makes a new Metadata instance.
func NewMetadata() Metadata {
	return Metadata{
		ColumnNames: make([]string, 0),
		ColumnTypes: make(map[string]string),
	}
}

// Append adds a new column name, type pair
func (md Metadata) Append(colName, colType string) Metadata {
	md.ColumnNames = append(md.ColumnNames, colName)
	md.ColumnTypes[colName] = colType

	return md
}

func (md Metadata) Len() int {
	return len(md.ColumnNames)
}

func (md Metadata) Name(i int) string {
	return md.ColumnNames[i]
}

func (md Metadata) Type(name string) (string, bool) {
	s, b := md.ColumnTypes[name]
	return s, b
}

// GetMetadata digs out the column names and data types from a query result.
func GetMetadata(rows *sql.Rows) Metadata {
	names, err := rows.Columns()
	FatalIfErr("getting column names", err)

	types, err := rows.ColumnTypes()
	FatalIfErr("getting datatypes", err)

	metadata := NewMetadata()

	for i, name := range names {
		metadata = metadata.Append(name, types[i].DatabaseTypeName())
	}

	return metadata
}

// DumpMetadata writes out column names and data types to the log.
func DumpMetadata(metadata Metadata) {
	for i := 0; i < metadata.Len(); i++ {
		n := metadata.Name(i)
		t, _ := metadata.Type(n)
		log.Printf("column %s is type %s", n, t)
	}
}
