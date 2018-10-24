package indexer

import (
	"encoding/json"
	"fmt"

	"github.com/CS-703059001-group4/blockchain-to-parquet/chain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcutil"
	"github.com/syndtr/goleveldb/leveldb"
)

type TxOut struct {
	Addresses []string
	Value     float64
}

type IndexerOptions struct {
	DataDir string
	DBPath  string
}

type Indexer struct {
	options *IndexerOptions
	db      *leveldb.DB
}

func New(options *IndexerOptions) *Indexer {
	return &Indexer{options, nil}
}

func (i *Indexer) openDB() error {
	if i.db == nil {
		db, err := leveldb.OpenFile(i.options.DBPath, nil)
		if err != nil {
			return err
		}
		i.db = db
	}
	return nil
}

func (i *Indexer) Destroy() error {
	if i.db != nil {
		return i.db.Close()
	}
	return nil
}

func (i *Indexer) Index(progress chan<- string, parallel int64) error {
	if err := i.openDB(); err != nil {
		return err
	}
	return chain.IterateTx(&chain.IterateTxOptions{
		FolderPath: i.options.DataDir,
		BufSize:    int(parallel),
		Parallel:   int(parallel),
		Handler: func(n int, tx *chain.Tx) error {
			hash := tx.Hash().String()
			defer func() {
				progress <- hash
			}()
			msgTx := tx.MsgTx()
			batch := new(leveldb.Batch)
			for i, txOut := range msgTx.TxOut {
				_, addrs, _, _ := txscript.ExtractPkScriptAddrs(txOut.PkScript, &chaincfg.MainNetParams)
				encodedAddrs := make([]string, len(addrs))
				for j, addr := range addrs {
					encodedAddrs[j] = addr.EncodeAddress()
				}
				val, err := json.Marshal(&TxOut{
					encodedAddrs,
					btcutil.Amount(txOut.Value).ToBTC(),
				})
				if err != nil {
					return err
				}
				batch.Put([]byte(fmt.Sprintf("txout-%s_%d", hash, i)), val)
			}
			return i.db.Write(batch, nil)
		},
	})
}
