package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/CS-703059001-group4/blockchain-to-parquet/converter"
	homedir "github.com/mitchellh/go-homedir"
)

var DataDir = flag.String("data", "~/Library/Application Support/Bitcoin/blocks", "")
var DateFile = flag.String("datefile", "./date.csv", "")
var EndBlock = flag.Uint("end", 546556, "")
var OutFile = flag.String("outfile", "./tx.parquet", "")
var Date = flag.String("date", "2018-10-20", "")

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
	outFile, err := homedir.Expand(*OutFile)
	if err != nil {
		log.Fatal(err)
	}
	date, err := time.Parse("2006-01-02", *Date)
	if err != nil {
		log.Fatal(err)
	}
	options := &converter.ConverterOptions{
		EndBlock: uint32(*EndBlock),
		DataDir:  dataDir,
		DateFile: dateFile,
		Parallel: int64(runtime.NumCPU()),
	}
	txConverter, err := converter.New(options)
	if err != nil {
		log.Fatal(err)
	}
	progressChan := make(chan float32, 100)
	defer close(progressChan)
	go func() {
		counter := 0
		for {
			progress, ok := <-progressChan
			if !ok {
				break
			}
			counter += 1
			fmt.Printf("\r%d, %d%%", counter, int(progress*100))
		}
	}()
	if err := txConverter.Convert(date, progressChan, outFile); err != nil {
		log.Fatal(err)
	}
}
