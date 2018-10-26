package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/CS-703059001-group4/blockchain-to-parquet/blockdate"
	"github.com/CS-703059001-group4/blockchain-to-parquet/chain"
	"github.com/btcsuite/btcutil"
	homedir "github.com/mitchellh/go-homedir"
)

var DataDir = flag.String("data", "./data", "")
var DateFile = flag.String("datefile", "./date.csv", "")
var By = flag.String("by", "date", "date or range")

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

	if len(os.Args) < 4 {
		flag.Usage()
	}

	date := blockdate.New()
	if err := date.Build(dateFile); err != nil {
		log.Fatal(err)
	}

	query := os.Args[3]
	var find chain.DatHandler
	switch *By {
	case "date":
		find = findByDate(date, query)
		break
	case "range":
		fmt.Println("NIY")
		os.Exit(1)
		break
	default:
		flag.Usage()
	}

	err = chain.IterateDat(&chain.IterateDatOptions{
		FolderPath: dataDir,
		BufSize:    10,
		Parallel:   10,
		Handler:    find,
	})

	if err != nil {
		log.Fatal(err)
	}
}

type blockHandler func(block *btcutil.Block) error

func eachBlock(dat *chain.Dat, handler blockHandler) error {
	for {
		blockBuf, err := dat.FetchNextBlock()
		if err != nil {
			return err
		}
		if blockBuf == nil {
			return nil
		}
		block, err := btcutil.NewBlockFromBytes(blockBuf)
		if err != nil {
			return err
		}
		if err := handler(block); err != nil {
			return err
		}
	}
}

var errSkip = errors.New("skip")

func findByDate(date *blockdate.BlockDate, query string) chain.DatHandler {
	targets := strings.Split(query, ",")
	targetsMap := map[string]int{}
	for _, target := range targets {
		targetsMap[target] = 1
	}
	return func(n int, dat *chain.Dat) error {
		err := eachBlock(dat, func(block *btcutil.Block) error {
			height, ok := date.GetHeight(block.Hash().String())
			if !ok {
				return nil
			}
			blockDate, ok := date.GetDate(height)
			if !ok {
				return nil
			}
			if _, ok := targetsMap[blockDate.Format("2006-01-02")]; ok {
				fmt.Println(dat.FilePath)
				return errSkip
			}
			return nil
		})
		if err != nil && err != errSkip {
			return err
		}
		return nil
	}
}
