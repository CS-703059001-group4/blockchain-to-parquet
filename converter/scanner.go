package converter

import (
	"encoding/hex"
	"time"

	"github.com/piotrnar/gocoin/lib/btc"
	"github.com/piotrnar/gocoin/lib/others/blockdb"
)

type scanner struct {
	db       *blockdb.BlockDB
	date     *blockDate
	endBlock uint32
	parallel int64
	stopped  bool
}

func loadDate(p *scanner, dateFile string) error {
	p.date = newBlockDate()
	return p.date.build(dateFile)
}

func (s *scanner) scan(targetDate time.Time, txChan chan<- *Tx, errChan chan<- error, stopChan <-chan struct{}, progressChan chan<- float32) {
	go func() {
		_, _ = <-stopChan
		s.stopped = true
	}()

	var height uint32
	var blockCounter uint32
	for height = 0; !s.stopped && height <= s.endBlock; height += 1 {
		err := s.scanBlock(txChan, targetDate)
		if err != nil {
			errChan <- err
			s.stopped = true
		}
		blockCounter += 1
		progressChan <- float32(blockCounter) / float32(s.endBlock-1)
	}
	errChan <- nil
}

func (s *scanner) scanBlock(txChan chan<- *Tx, targetDate time.Time) error {
	dat, err := s.db.FetchNextBlock()
	if err != nil {
		return err
	}
	block, err := btc.NewBlock(dat[:])
	if err != nil {
		return err
	}
	height, ok := s.date.getHeight(block.Hash.String())
	if !ok {
		return nil
	}
	blockDate, ok := s.date.getDate(height)
	if !ok {
		return nil
	}
	if blockDate.Before(targetDate) {
		return nil
	}
	err = block.BuildTxList()
	if err != nil {
		return err
	}
	for _, tx := range block.Txs {
		scannerTx, err := s.scanTx(tx, block.Height, blockDate)
		if err != nil {
			return err
		}
		if !s.stopped {
			txChan <- scannerTx
		}
	}
	return nil
}

func (s *scanner) scanTx(tx *btc.Tx, height uint32, blockDate time.Time) (*Tx, error) {
	scannerTx := &Tx{
		Hash:         tx.Hash.String(),
		Block:        int32(height),
		Size:         int32(tx.Size),
		LockTime:     int32(tx.Lock_time),
		ReceivedTime: blockDate.Unix(),
		TotalOutput:  0,
		Vin:          []TxIn{},
		Vout:         []TxOut{},
	}
	for _, txIn := range tx.TxIn {
		if !txIn.Input.IsNull() {
			scannerTx.Vin = append(scannerTx.Vin, TxIn{
				PrevHash: hex.EncodeToString(txIn.Input.Hash[:]),
				Index:    int32(txIn.Input.Vout),
			})
		}
	}
	for _, txOut := range tx.TxOut {
		value := float64(txOut.Value) / 1e8
		scannerTx.TotalOutput += value
		btcAddr := btc.NewAddrFromPkScript(txOut.Pk_script, false)
		addr := ""
		if btcAddr != nil {
			addr = btcAddr.String()
		}
		scannerTx.Vout = append(scannerTx.Vout, TxOut{
			Address: addr,
			Value:   value,
		})
	}
	return scannerTx, nil
}
