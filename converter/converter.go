package converter

import (
	"path"

	"github.com/CS-703059001-group4/blockchain-to-parquet/chain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcutil"
)

type ConverterOptions struct {
	OutputDir string
	DataDir   string
	DateFile  string
}

type Converter struct {
	options *ConverterOptions
	date    *blockDate
}

func New(options *ConverterOptions) (*Converter, error) {
	date := newBlockDate()
	if err := date.build(options.DateFile); err != nil {
		return nil, err
	}
	return &Converter{options, date}, nil
}

func (c *Converter) Convert(progress chan<- string, parallel int64) error {
	files := newOutputFiles(path.Join(c.options.OutputDir, "/%d.parquet"), parallel)
	return chain.IterateTx(&chain.IterateTxOptions{
		FolderPath: c.options.DataDir,
		BufSize:    int(parallel),
		Parallel:   int(parallel),
		Handler: func(n int, rawTx *chain.Tx) error {
			hash := rawTx.Hash().String()
			defer func() {
				progress <- hash
			}()
			msgTx := rawTx.MsgTx()
			height, ok := c.date.getHeight(rawTx.Block)
			if !ok {
				return nil
			}
			date, ok := c.date.getDate(height)
			if !ok {
				return nil
			}
			tx := &Tx{
				Hash:         hash,
				LockTime:     int32(msgTx.LockTime),
				Size:         int32(msgTx.SerializeSize()),
				ReceivedTime: date.Unix(),
				Block:        int32(height),
				TotalOutput:  0,
				Vin:          make([]TxIn, len(msgTx.TxIn)),
				Vout:         make([]TxOut, len(msgTx.TxOut)),
			}
			for i, txIn := range msgTx.TxIn {
				tx.Vin[i] = TxIn{
					PrevHash: txIn.PreviousOutPoint.Hash.String(),
					Index:    int32(txIn.PreviousOutPoint.Index),
				}
			}
			for i, txOut := range msgTx.TxOut {
				scriptClass, addrs, _, _ := txscript.ExtractPkScriptAddrs(txOut.PkScript, &chaincfg.MainNetParams)
				encodedAddrs := make([]string, len(addrs))
				for j, addr := range addrs {
					encodedAddrs[j] = addr.EncodeAddress()
				}
				val := btcutil.Amount(txOut.Value).ToBTC()
				tx.TotalOutput += val
				tx.Vout[i] = TxOut{
					Addresses: encodedAddrs,
					Value:     val,
					Type:      scriptClass.String(),
				}
			}
			return files.write(tx)
		},
	})
}
