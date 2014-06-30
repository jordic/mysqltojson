package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jordic/mysqltojson/mysqlquery"
	"net"
	"os"
)

var (
	// db params
	user   = flag.String("user", "", "mysql database username")
	pass   = flag.String("pass", "", "mysql password")
	prot   = flag.String("prot", "tcp", "mysql connection protocol")
	addr   = flag.String("addr", "localhost:3306", "mysql server address")
	dbname = flag.String("db", "", "mysqldbname")
	// app params
	query = flag.String("query", "", "table query")
	table = flag.String("table", "", "Table name to dump")
)

func out_error(msg string) {

	fmt.Printf("%s\n", msg)
	os.Exit(0)

}

func DumpTableToJson() {

	// check params
	if *user == "" || *dbname == "" {
		out_error("You must supply a user, password and dbname")
	}
	netAddr := fmt.Sprintf("%s(%s)", *prot, *addr)
	dsn := fmt.Sprintf("%s:%s@%s/%s?timeout=30s&strict=true", *user, *pass, netAddr, *dbname)

	c, err := net.Dial(*prot, *addr)
	if err != nil {
		out_error("Unable to connect to db")
		c.Close()
	}

	// database connect
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		out_error("Unable to connect to db. Incorrect user/password?")
	}
	defer db.Close()

	if *table == "" {
		out_error("Which table should i use?")
	}

	qu := &mysqlquery.MysqlQuery{
		Db:    db,
		Table: *table,
		Cols:  make(map[string]string),
	}

	if *query != "" {
		qu.Query = *query
	}

	err = qu.GetTypesFromTable()
	if err != nil {
		out_error(fmt.Sprintf("Can't get col descriptions %s", err))
	}

	err = qu.GetData()
	if err != nil {
		out_error(fmt.Sprintf("Can't get data from table %s", err))
	}

	//fmt.Printf("%#v", qu)

	res, err := json.Marshal(qu.Result)
	if err != nil {
		out_error(fmt.Sprintf("Unable to serialize result %s", err))
	}

	os.Stdout.Write(res)

}
