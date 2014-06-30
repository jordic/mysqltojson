package main

import (
	"flag"
	"fmt"
	//"github.com/jordic/mysqltojson/mysqlquery"
	"os"
)

var (
	version string = "0.2"
	config         = flag.String("config", "", "path to config file")
	web            = flag.Bool("websrv", false, "Serve content as web service")
	ver            = flag.Bool("v", false, "Version number")
)

func main() {

	flag.Parse()

	if *ver {
		fmt.Printf("Version %s\n", version)
		os.Exit(0)
	}

	if !*web {
		DumpTableToJson()
		return
	}

}
