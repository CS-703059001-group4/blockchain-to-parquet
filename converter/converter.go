package converter

import (
	"runtime"
	"time"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
)

type Converter struct {
	rpc      *rpcclient.Client
	date     *blockDate
	parallel int64
}

type ConverterOptions struct {
	Host     string
	User     string
	Pass     string
	DateFile string
	Parallel int64
}

func New(options *ConverterOptions) (*Converter, error) {
	connCfg := &rpcclient.ConnConfig{
		Host:         options.Host,
		User:         options.User,
		Pass:         options.Pass,
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		return nil, err
	}
	date := newBlockDate()
	err = date.build(options.DateFile)
	if err != nil {
		return nil, err
	}
	return &Converter{
		client,
		date,
		int64(runtime.NumCPU()),
	}, nil
}

func (c *Converter) Convert(targetTime time.Time, progress chan<- *Tx, outFile string) error {
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
	txScanner := &scanner{c.rpc, c.date, c.parallel}
	go txScanner.scan(targetTime, txChan, errChan, stopChan)

	stop := false
	for !stop {
		select {
		case tx := <-txChan:
			progress <- tx
			if err = writer.Write(tx); err != nil {
				stopChan <- struct{}{}
				stop = true
			}
			break
		case err = <-errChan:
			break
		}
	}

	if err != nil {
		writer.WriteStop()
		return err
	}

	return writer.WriteStop()
}
