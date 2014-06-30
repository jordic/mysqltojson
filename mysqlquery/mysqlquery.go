package mysqlquery

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
	"time"
)

type MysqlQuery struct {
	db     *sql.DB
	Query  string
	Cols   map[string]string
	Table  string
	Result []*RRow
}

type RRow map[string]interface{}

func (t MysqlQuery) GetData() error {
	var query string
	if t.Query != "" {
		query = t.Query
	} else {
		query = fmt.Sprintf("SELECT * FROM %s", t.Table)
	}

	rows, err := t.db.Query(query)
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	vs := make([]sql.RawBytes, len(columns))
	values := make(map[string]*sql.RawBytes)
	for i := range columns {
		values[columns[i]] = &vs[i]
		//fmt.Printf("%s, %s", columns[i], vs[i])
	}

	//values := make([]*sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(columns))

	for i := range columns {
		scanArgs[i] = values[columns[i]]
	}

	for rows.Next() {
		var r = make(RRow)
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}

		for c, val := range values {
			//fmt.Printf("%s, %#v", c, val)
			if t.Cols[c] != "bytes" {
				r[c], err = TypeConvert(string(*val), t.Cols[c])
				if err != nil {
					return err
				}
			} else {
				r[c] = *val
			}
		}

		//fmt.Printf("%#v", r)
		t.Result = append(t.Result, &r)

	}

	return nil

}

// GetTypesFromTable scans mysql table for trying to know
// which types must export during json marshall
func (t MysqlQuery) GetTypesFromTable() error {
	query := fmt.Sprintf("DESCRIBE %s", t.Table)
	rows, err := t.db.Query(query)
	if err != nil {
		return err
	}

	var (
		field string
		tipo  string
		null  sql.NullString
		key   sql.NullString
		def   sql.NullString
		extra sql.NullString
	)

	for rows.Next() {
		err = rows.Scan(&field, &tipo, &key, &null, &def, &extra)
		if err != nil {
			return err
		}
		//columns = append(columns, field)
		t.Cols[field] = genericTypeMapper(tipo)
	}
	return nil
}

// maps a mysql col type to a golang type
func genericTypeMapper(s string) string {

	// Can't be parsed by table because are ambiguous datetime=date
	if strings.HasPrefix(s, "datetime") {
		return "datetime"
	}
	if strings.HasPrefix(s, "timestamp") {
		return "timestamp"
	}

	for k, v := range TypesMaps {
		if strings.HasPrefix(s, k) {
			return v
		}
	}

	return "string"
}

var TypesMaps = map[string]string{
	"tinyint":    "int32",
	"smallint":   "int32",
	"mediumint":  "int64",
	"int":        "int64",
	"bigint":     "int64",
	"float":      "float64",
	"double":     "float64",
	"decimal":    "float64",
	"bit":        "bytes",
	"char":       "string",
	"varchar":    "string",
	"tinytext":   "string",
	"text":       "string",
	"mediumtext": "string",
	"longtext":   "string",
	"binary":     "bytes",
	"varbinary":  "bytes",
	"tinyblob":   "bytes",
	"blob":       "bytes",
	"mediumblob": "bytes",
	"longblob":   "bytes",
	"enum":       "string",
	"set":        "string",
	"date":       "date",
	"time":       "time",
	"year":       "int32",
}

// Type convert, returns the string value converted to the
// tipo param...
func TypeConvert(val string, tipo string) (interface{}, error) {

	if tipo == "int64" {
		res, e := strconv.ParseInt(val, 0, 64)
		return res, e
	}

	if tipo == "int32" {
		res, e := strconv.ParseInt(val, 0, 32)
		return int32(res), e
	}

	if tipo == "float64" {
		res, e := strconv.ParseFloat(val, 64)
		return res, e
	}

	if tipo == "date" {
		res, e := time.Parse("2006-01-02", val)
		return res, e
	}

	if tipo == "datetime" {
		res, e := time.Parse("2006-01-02 15:04:05", val)
		return res, e
	}

	if tipo == "time" {
		res, e := time.Parse("15:04:05", val)
		return res, e
	}

	if tipo == "timestamp" {
		t, e := strconv.ParseInt(val, 0, 64)
		res := time.Unix(t/1000, 0)
		return res, e
	}

	return val, nil

}
