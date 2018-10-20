package blockdate

import (
	"fmt"
	"io"
)

func Fetch(writer io.Writer) error {
	btc := &BtcCom{}
	page := btc.LastPage()
	for page != nil {
		err := page.Fetch()
		if err != nil {
			return err
		}
		for height, meta := range page.Blocks {
			writer.Write([]byte(fmt.Sprintf("%d,%s,%d\n", height, meta.Hash, meta.ReceivedTime.Unix())))
		}
		page = page.PrevPage()
	}
	return nil
}
