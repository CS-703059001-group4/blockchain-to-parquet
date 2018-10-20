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
		for height, blockTime := range page.Blocks {
			writer.Write([]byte(fmt.Sprintf("%d,%d\n", height, blockTime.Unix())))
		}
		page = page.PrevPage()
	}
	return nil
}
