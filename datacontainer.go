package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/lib/pq"
)

// DataContainer holds a generic result set, and some metadata about what's in it
type DataContainer struct {
	Metadata
	Values map[string]interface{}
}

func (data DataContainer) GetString(colName string) (sql.NullString, error) {
	t, found := data.Type(colName)
	if !found {
		return sql.NullString{}, fmt.Errorf("no column named %s in data", colName)
	}

	if t != "VARCHAR" {
		return sql.NullString{}, fmt.Errorf("cannot retrieve string for column %s of type %s", colName, t)
	}

	v := data.Values[colName].(*sql.NullString)

	return *v, nil
}

func (data DataContainer) GetBool(colName string) (sql.NullBool, error) {
	t, found := data.Type(colName)
	if !found {
		return sql.NullBool{}, fmt.Errorf("no column named %s in data", colName)
	}

	if t != "BOOL" {
		return sql.NullBool{}, fmt.Errorf("cannot retrieve string for column %s of type %s", colName, t)
	}

	v := data.Values[colName].(*sql.NullBool)

	return *v, nil
}

func (data DataContainer) GetDate(colName string) (pq.NullTime, error) {
	t, found := data.Type(colName)
	if !found {
		return pq.NullTime{}, fmt.Errorf("no column named %s in data", colName)
	}

	if t != "DATE" {
		return pq.NullTime{}, fmt.Errorf("cannot retrieve date for column %s of type %s", colName, t)
	}

	v := data.Values[colName].(*pq.NullTime)

	return *v, nil
}

func (data DataContainer) GetTimestamp(colName string) (pq.NullTime, error) {
	t, found := data.Type(colName)
	if !found {
		return pq.NullTime{}, fmt.Errorf("no column named %s in data", colName)
	}

	if t != "TIMESTAMP" {
		return pq.NullTime{}, fmt.Errorf("cannot retrieve timestamp for column %s of type %s", colName, t)
	}

	v := data.Values[colName].(*pq.NullTime)

	return *v, nil
}

func (data DataContainer) GetNumeric(colName string) (sql.NullFloat64, error) {
	t, found := data.Type(colName)
	if !found {
		return sql.NullFloat64{}, fmt.Errorf("no column named %s in data", colName)
	}

	if t != "NUMERIC" {
		return sql.NullFloat64{}, fmt.Errorf("cannot retrieve numeric for column %s of type %s", colName, t)
	}

	v := data.Values[colName].(*sql.NullFloat64)

	return *v, nil
}

func (data DataContainer) GetInt(colName string) (sql.NullInt64, error) {
	t, found := data.Type(colName)
	if !found {
		return sql.NullInt64{}, fmt.Errorf("no column named %s in data", colName)
	}

	if t != "INT8" {
		return sql.NullInt64{}, fmt.Errorf("cannot retrieve XXX for column %s of type %s", colName, t)
	}

	v := data.Values[colName].(*sql.NullInt64)

	return *v, nil
}

// ScanRow scans a single row from a query result into a DataContainer, and returns it.
func ScanRow(row *sql.Rows, metadata Metadata) DataContainer {
	data := DataContainer{
		Metadata: metadata,
		Values:   make(map[string]interface{}),
	}

	scanArgs := make([]interface{}, 0, metadata.Len())
	for _, colName := range metadata.ColumnNames {
		colType, _ := metadata.Type(colName)

		switch colType {
		case "VARCHAR":
			v := new(sql.NullString)
			scanArgs = append(scanArgs, v)
		case "BOOL":
			b := new(sql.NullBool)
			scanArgs = append(scanArgs, b)
		case "DATE":
			d := new(pq.NullTime)
			scanArgs = append(scanArgs, d)
		case "TIMESTAMP":
			d := new(pq.NullTime)
			scanArgs = append(scanArgs, d)
		case "NUMERIC":
			n := new(sql.NullFloat64)
			scanArgs = append(scanArgs, n)
		case "INT8":
			i := new(sql.NullInt64)
			scanArgs = append(scanArgs, i)
		default:
			log.Printf("unhandled data type %s for column %s, using string", colType, colName)
			s := new(sql.NullString)
			scanArgs = append(scanArgs, s)
		}
	}

	err := row.Scan(scanArgs...)
	FatalIfErr("scanning query result", err)

	for i, colName := range metadata.ColumnNames {
		data.Values[colName] = scanArgs[i]
	}

	return data
}
