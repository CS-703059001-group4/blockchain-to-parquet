package chain

import (
	"context"

	"github.com/btcsuite/btcutil"
)

type Tx struct {
	*btcutil.Tx
	Block string
}

func ExtractTx(ctx context.Context, datChan <-chan *Dat, bufSize int) (<-chan *Tx, <-chan error) {
	txChan := make(chan *Tx)
	errChan := make(chan error, 1)

	go func() {
		defer func() {
			close(txChan)
			errChan <- nil
		}()
		for dat := range datChan {
			for {
				blockBuf, err := dat.FetchNextBlock()
				if err != nil {
					errChan <- err
					return
				}
				if blockBuf == nil {
					break
				}
				block, err := btcutil.NewBlockFromBytes(blockBuf)
				if err != nil {
					errChan <- err
					return
				}
				for _, btcTx := range block.Transactions() {
					tx := &Tx{
						btcTx,
						block.Hash().String(),
					}
					select {
					case <-ctx.Done():
					case txChan <- tx:
						break
					}
				}
			}
		}
	}()

	return txChan, errChan
}
