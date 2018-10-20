package converter

import (
	"time"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire"
)

type scanner struct {
	rpc      *rpcclient.Client
	date     *blockDate
	parallel int64
}

func loadDate(p *scanner, dateFile string) error {
	p.date = newBlockDate()
	return p.date.build(dateFile)
}

func (s *scanner) scan(targetDate time.Time, txChan chan<- *Tx, errChan chan<- error, stopChan <-chan struct{}) {
	count, err := s.rpc.GetBlockCount()
	if err != nil {
		errChan <- err
		return
	}
	stopped := false
	go func() {
		_, _ = <-stopChan
		stopped = true
	}()
	concurrent := make(chan int, s.parallel)
	defer close(concurrent)
	for height := count; !stopped && height >= 0; height -= 1 {
		blockDate, ok := s.date.getDate(uint32(height))
		if !ok {
			continue
		}
		if blockDate.Before(targetDate) {
			continue
		}
		hash, err := s.rpc.GetBlockHash(height)
		if err != nil {
			errChan <- err
			return
		}
		block, err := s.rpc.GetBlock(hash)
		if err != nil {
			errChan <- err
			return
		}
		for _, msgTx := range block.Transactions {
			concurrent <- 0
			go func() {
				scannerTx, err := s.scanTx(msgTx, height, blockDate)
				defer func() { <-concurrent }()
				if err != nil {
					errChan <- err
				}
				if !stopped {
					txChan <- scannerTx
				}
			}()
		}
	}
	errChan <- nil
}

func (s *scanner) scanTx(msgTx *wire.MsgTx, height int64, blockDate time.Time) (*Tx, error) {
	txHash := msgTx.TxHash()
	tx, err := s.rpc.GetRawTransactionVerbose(&txHash)
	if err != nil {
		return nil, err
	}
	scannerTx := &Tx{
		Hash:         txHash.String(),
		Block:        height,
		LockTime:     tx.Size,
		ReceivedTime: blockDate.Unix(),
		TotalOutput:  0,
		Vin:          []TxIn{},
		Vout:         []TxOut{},
	}
	for _, txIn := range msgTx.TxIn {
		scannerTx.Vin = append(scannerTx.Vin, TxIn{
			PrevHash: (&txIn.PreviousOutPoint.Hash).String(),
			Index:    int32(txIn.PreviousOutPoint.Index),
		})
	}
	for _, txOut := range tx.Vout {
		scannerTx.TotalOutput += txOut.Value
		scannerTx.Vout = append(scannerTx.Vout, TxOut{
			Addresses: txOut.ScriptPubKey.Addresses[:],
			Value:     txOut.Value,
			Type:      txOut.ScriptPubKey.Type,
		})
	}
	return scannerTx, nil
}
