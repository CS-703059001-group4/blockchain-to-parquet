package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/CS-703059001-group4/blockchain-to-parquet/converter"
	homedir "github.com/mitchellh/go-homedir"
)

var DataDir = flag.String("data", "./data", "")
var DateFile = flag.String("datefile", "./date.csv", "")
var OutDir = flag.String("out", "./out", "")

func main() {
	flag.Parse()
	dataDir, err := homedir.Expand(*DataDir)
	if err != nil {
		log.Fatal(err)
	}
	dateFile, err := homedir.Expand(*DateFile)
	if err != nil {
		log.Fatal(err)
	}
	outDir, err := homedir.Expand(*OutDir)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	options := &converter.ConverterOptions{
		DataDir:   dataDir,
		DateFile:  dateFile,
		OutputDir: outDir,
	}
	txConverter, err := converter.New(options)
	if err != nil {
		log.Fatal(err)
	}
	defer txConverter.Destroy()

	progress := make(chan string, 100)
	defer close(progress)
	go func() {
		for txHash := range progress {
			fmt.Printf("\r%s", txHash)
		}
	}()
	if err := txConverter.Convert(progress, int64(runtime.NumCPU())); err != nil {
		log.Fatal(err)
	}
}
