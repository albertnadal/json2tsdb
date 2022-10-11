package main

import (
	"log"
	"os"
	"encoding/json"
	"json2tsdb/tsdb"
)


func main() {
	log.Printf("Generate Prometheus TSDB data.")

	config := tsdb.Configuration{}
	configValues := os.Getenv("METRICS_CONFIG")

	if err := json.Unmarshal([]byte(configValues), &config); err != nil {
		log.Fatalf("Error loading configuration from 'METRICS_CONFIG' venv: %s", err)
		panic(err)
	}

	if err := tsdb.CreateThanosTSDB(config); err != nil {
		log.Fatalf("Error generating TSDB data: %s", err)
        panic(err)
    }

	log.Printf("TSDB data generation complete")
}
