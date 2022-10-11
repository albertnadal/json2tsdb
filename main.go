package main

import (
	"flag"
	"log"
	"encoding/json"
	"io/ioutil"
	"json2tsdb/tsdb"
)

var (
	configFile = flag.String("i", "config.json",
		"Configuration file")
)

func main() {
	log.Printf("Generate Prometheus TSDB data.")
	flag.Parse()

	file, _ := ioutil.ReadFile(*configFile)
	config := tsdb.Configuration{}

	if err := json.Unmarshal([]byte(file), &config); err != nil {
		log.Fatalf("Error loading configuration file: %s", err)
        panic(err)
    }

	if err := tsdb.CreateThanosTSDB(config); err != nil {
		log.Fatalf("Error generating TSDB data: %s", err)
        panic(err)
    }

	log.Printf("TSDB data generation complete")
}
