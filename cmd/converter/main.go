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

var Host = flag.String("host", "localhost:8332", "")
var User = flag.String("user", "user", "")
var Pass = flag.String("pass", "123", "")
var DateFile = flag.String("datefile", "./date.csv", "")
var OutFile = flag.String("outfile", "./tx.parquet", "")
var Date = flag.String("date", "2018-10-20", "")

func main() {
	flag.Parse()
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
		Host:     *Host,
		User:     *User,
		Pass:     *Pass,
		DateFile: dateFile,
		Parallel: int64(runtime.NumCPU() * 2),
	}
	txConverter, err := converter.New(options)
	if err != nil {
		log.Fatal(err)
	}
	progressChan := make(chan *converter.Tx, 100)
	defer close(progressChan)
	go func() {
		counter := 0
		for {
			tx, ok := <-progressChan
			if !ok {
				break
			}
			counter += 1
			fmt.Printf("\r%s (block: %d, counter: %d)", time.Unix(tx.ReceivedTime, 0), tx.Block, counter)
		}
	}()
	if err := txConverter.Convert(date, progressChan, outFile); err != nil {
		log.Fatal(err)
	}
}
