package main

import (
	"log"
	"os"

	"github.com/CS-703059001-group4/blockchain-to-parquet/blockdate"
)

func main() {
	err := blockdate.Fetch(os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
}
