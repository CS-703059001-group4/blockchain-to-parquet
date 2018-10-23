package chain

import (
	"context"
	"sync"
)

type txHandler func(int, *Tx) error

type IterateTxOptions struct {
	FolderPath string
	Handler    txHandler
	BufSize    int
	Parallel   int
}

func IterateTx(options *IterateTxOptions) error {
	var wg sync.WaitGroup
	var err error
	errcList := [](<-chan error){}
	ctx, cancel := context.WithCancel(context.Background())

	// define pipeline
	datc, errc := ScanFolder(ctx, options.FolderPath, options.BufSize)
	errcList = append(errcList, errc)

	txc, errc := ExtractTx(ctx, datc, options.BufSize)
	errcList = append(errcList, errc)

	// cancel all goroutines when error occurred
	go func() {
		defer cancel()
		for _, errc := range errcList {
			pipeLineErr := <-errc
			if pipeLineErr != nil {
				err = pipeLineErr
				return
			}
		}
	}()

	// pass transactions to transaction handler
	handleTx := func(n int) {
		defer wg.Done()
		for tx := range txc {
			if txErr := options.Handler(n, tx); txErr != nil {
				err = txErr
				cancel()
				return
			}
		}
	}
	for i := 0; i < options.Parallel; i++ {
		wg.Add(1)
		go handleTx(i)
	}

	// wait until all goroutines finish their jobs
	wg.Wait()

	return err
}
