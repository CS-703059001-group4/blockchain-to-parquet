package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"

	"github.com/CS-703059001-group4/blockchain-to-parquet/indexer"
	homedir "github.com/mitchellh/go-homedir"
)

var DataDir = flag.String("data", "./data", "")
var DBPath = flag.String("db", "./db", "")

func main() {
	flag.Parse()
	dataDir, err := homedir.Expand(*DataDir)
	if err != nil {
		log.Fatal(err)
	}
	dbPath, err := homedir.Expand(*DBPath)
	if err != nil {
		log.Fatal(err)
	}
	options := &indexer.IndexerOptions{
		dataDir,
		dbPath,
	}
	txIndexer := indexer.New(options)
	defer txIndexer.Destroy()
	progress := make(chan string, 100)
	defer close(progress)
	go func() {
		for txHash := range progress {
			fmt.Printf("\r%s", txHash)
		}
	}()
	txIndexer.Index(progress, int64(runtime.NumCPU()))
}
