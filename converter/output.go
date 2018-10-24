package converter

import (
	"fmt"
	"sync"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
)

type outputFile struct {
	parquetFile   ParquetFile.ParquetFile
	parquetWriter *ParquetWriter.ParquetWriter
}

func newOutputFile(filePath string, parallel int64) (*outputFile, error) {
	file, err := ParquetFile.NewLocalFileWriter(filePath)
	if err != nil {
		return nil, err
	}
	writer, err := ParquetWriter.NewParquetWriter(file, new(Tx), parallel)
	if err != nil {
		return nil, err
	}
	return &outputFile{
		file,
		writer,
	}, nil
}

func (of *outputFile) write(tx *Tx) error {
	return of.parquetWriter.Write(tx)
}

func (of *outputFile) close() error {
	defer of.parquetFile.Close()
	return of.parquetWriter.WriteStop()
}

type outputFiles struct {
	fileNameFormat string
	sizePerFile    int32
	currentFile    *outputFile
	currentSize    int32
	index          int
	lock           sync.Mutex
	parallel       int64
}

func newOutputFiles(fileNameFormat string, parallel int64) *outputFiles {
	return &outputFiles{
		fileNameFormat,
		128000000,
		nil,
		0,
		0,
		sync.Mutex{},
		parallel,
	}
}

func (ofs *outputFiles) close() error {
	ofs.lock.Lock()
	defer ofs.lock.Unlock()
	return ofs.currentFile.close()
}

func (ofs *outputFiles) write(tx *Tx) error {
	ofs.lock.Lock()
	defer ofs.lock.Unlock()
	file, err := ofs.getFile()
	if err != nil {
		return err
	}
	ofs.currentSize += tx.Size
	return file.write(tx)
}

func (ofs *outputFiles) getFile() (*outputFile, error) {
	if ofs.currentSize >= ofs.sizePerFile {
		if err := ofs.currentFile.close(); err != nil {
			return nil, err
		}
	}
	if ofs.currentFile == nil || ofs.currentSize >= ofs.sizePerFile {
		file, err := newOutputFile(fmt.Sprintf(ofs.fileNameFormat, ofs.index), ofs.parallel)
		if err != nil {
			return nil, err
		}
		ofs.currentFile = file
		ofs.currentSize = 0
		ofs.index++
	}
	return ofs.currentFile, nil
}
