package converter

type Tx struct {
	Hash         string  `parquet:"name=hash, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LockTime     int32   `parquet:"name=lock_time, type=INT32"`
	Size         int32   `parquet:"name=size, type=INT32"`
	ReceivedTime int64   `parquet:"name=received_time, type=INT64"`
	TotalOutput  float64 `parquet:"name=total_output, type=DOUBLE"`
	Block        int32   `parquet:"name=block, type=INT32"`
	Fee          int64   `parquet:"name=fee, type=INT64"`
	Vin          []TxIn  `parquet:"name=vin, repetitiontype=REPEATED"`
	Vout         []TxOut `parquet:"name=vout, repetitiontype=REPEATED"`
}

type TxOut struct {
	Address string  `parquet:"name=address, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Value   float64 `parquet:"name=value, type=DOUBLE"`
}

type TxIn struct {
	PrevHash string `parquet:"name=prev_hash, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Index    int32  `parquet:"name=index, type=INT32"`
}
