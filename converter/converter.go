package converter

import (
	"time"

	"github.com/piotrnar/gocoin/lib/others/blockdb"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
)

type Converter struct {
	db       *blockdb.BlockDB
	date     *blockDate
	endBlock uint32
	parallel int64
}

type ConverterOptions struct {
	EndBlock uint32
	DataDir  string
	DateFile string
	Parallel int64
}

func New(options *ConverterOptions) (*Converter, error) {
	magic := [4]byte{0xF9, 0xBE, 0xB4, 0xD9}
	db := blockdb.NewBlockDB(options.DataDir, magic)
	date := newBlockDate()
	err := date.build(options.DateFile)
	if err != nil {
		return nil, err
	}
	return &Converter{
		db,
		date,
		options.EndBlock,
		options.Parallel,
	}, nil
}

func (c *Converter) Convert(targetTime time.Time, progressChan chan<- float32, outFile string) error {
	// init chan
	txChan := make(chan *Tx, 100)
	defer close(txChan)
	errChan := make(chan error)
	defer close(errChan)
	stopChan := make(chan struct{})
	defer close(stopChan)

	// create parquet writer
	fileWriter, err := ParquetFile.NewLocalFileWriter(outFile)
	if err != nil {
		return err
	}
	defer fileWriter.Close()
	writerFile := ParquetFile.NewWriterFile(fileWriter)
	writer, err := ParquetWriter.NewParquetWriter(writerFile, new(Tx), c.parallel)
	if err != nil {
		return err
	}
	writer.RowGroupSize = 128 * 1024 * 1024
	writer.CompressionType = parquet.CompressionCodec_SNAPPY

	// create scanner
	txScanner := &scanner{c.db, c.date, c.endBlock, c.parallel, false}
	go txScanner.scan(targetTime, txChan, errChan, stopChan, progressChan)

	stop := false
	for !stop {
		select {
		case tx := <-txChan:
			if err = writer.Write(tx); err != nil {
				stopChan <- struct{}{}
				stop = true
			}
			break
		case err = <-errChan:
			stop = true
			break
		}
	}

	if err != nil {
		writer.WriteStop()
		return err
	}

	return writer.WriteStop()
}
