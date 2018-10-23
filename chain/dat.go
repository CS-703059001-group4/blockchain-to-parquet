package chain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
)

type Dat struct {
	FilePath string
	File     *os.File
}

func NewDat(filePath string) (*Dat, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return &Dat{
		filePath,
		file,
	}, nil
}

func (d *Dat) FetchNextBlock() ([]byte, error) {
	ok, err := checkMagic(d.File)
	if err == io.EOF {
		return nil, nil
	}
	if !ok {
		return nil, errors.New("Unexpected magic")
	}

	size, err := getBlockSize(d.File)
	if err != nil {
		return nil, err
	}

	if size < 81 {
		return nil, errors.New(fmt.Sprintf("Incorrect block size: %d", size))
	}

	block := make([]byte, size)
	_, err = d.File.Read(block[:])

	return block, err
}

func ScanFolder(ctx context.Context, folderPath string, bufSize int) (<-chan *Dat, <-chan error) {
	datChan := make(chan *Dat)
	errChan := make(chan error, 1)

	go func() {
		defer func() {
			close(datChan)
			errChan <- nil
		}()

		filePathChan, err := scanFiles(folderPath, bufSize)
		if err != nil {
			errChan <- err
			return
		}

		for filePath := range filePathChan {
			dat, err := NewDat(filePath)
			if err != nil {
				errChan <- err
				break
			}
			select {
			case datChan <- dat:
			case <-ctx.Done():
				return
			}
		}
	}()

	return datChan, errChan
}

func checkMagic(file *os.File) (bool, error) {
	magic := [4]byte{0xF9, 0xBE, 0xB4, 0xD9}
	var buf [4]byte
	if _, err := file.Read(buf[:]); err != nil {
		return false, err
	}
	if !bytes.Equal(magic[:], buf[:]) {
		if bytes.Equal(buf[:], []byte{0, 0, 0, 0}) {
			return false, io.EOF
		}
		return false, nil
	}
	return true, nil
}

func getBlockSize(file *os.File) (uint32, error) {
	var buf [4]byte
	if _, err := file.Read(buf[:]); err != nil {
		return 0, err
	}
	return lsb2uint(buf[:]), nil
}

func lsb2uint(lt []byte) uint32 {
	var res uint64
	for i := 0; i < len(lt); i++ {
		res |= (uint64(lt[i]) << uint(i*8))
	}
	return uint32(res)
}

func scanFiles(folderPath string, bufSize int) (<-chan string, error) {
	matches, err := filepath.Glob(path.Join(folderPath, "/*.dat"))
	if err != nil {
		return nil, err
	}
	filePathChan := make(chan string, bufSize)
	go func() {
		defer close(filePathChan)
		for _, filePath := range matches {
			filePathChan <- filePath
		}
	}()
	return filePathChan, nil
}
