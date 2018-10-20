package converter

import (
	"time"

	"github.com/btcsuite/btcd/rpcclient"
)

type scanner struct {
	rpc  *rpcclient.Client
	date *blockDate
}

func loadDate(p *scanner, dateFile string) error {
	p.date = newBlockDate()
	return p.date.build(dateFile)
}

func (p *scanner) scan(targetDate time.Time, txChan chan<- Tx, errChan chan<- error, stopChan <-chan struct{}) {
	count, err := p.rpc.GetBlockCount()
	if err != nil {
		errChan <- err
		return
	}
	stopped := false
	go func() {
		_, _ = <-stopChan
		stopped = true
	}()
	for height := count; !stopped && height >= 0; height -= 1 {
		blockDate, ok := p.date.getDate(uint32(height))
		if !ok {
			continue
		}
		if blockDate.Before(targetDate) {
			continue
		}
		hash, err := p.rpc.GetBlockHash(height)
		if err != nil {
			errChan <- err
			return
		}
		block, err := p.rpc.GetBlock(hash)
		if err != nil {
			errChan <- err
			return
		}
		for _, msgTx := range block.Transactions {
			txHash := msgTx.TxHash()
			tx, err := p.rpc.GetRawTransactionVerbose(&txHash)
			if err != nil {
				errChan <- err
				return
			}
			scannerTx := Tx{
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
			if !stopped {
				txChan <- scannerTx
			}
		}
	}
	errChan <- nil
}
