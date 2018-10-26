package blockdate

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
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

type BlockDate struct {
	height map[uint32]time.Time
	hash   map[string]uint32
}

func New() *BlockDate {
	return &BlockDate{
		map[uint32]time.Time{},
		map[string]uint32{},
	}
}

func (d *BlockDate) GetHeight(hash string) (uint32, bool) {
	height, ok := d.hash[hash]
	return height, ok
}

func (d *BlockDate) GetDate(height uint32) (time.Time, bool) {
	date, ok := d.height[height]
	return date, ok
}

func (d *BlockDate) Build(dateFile string) error {
	file, err := os.Open(dateFile)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ",")
		height, _ := strconv.ParseUint(parts[0], 10, 32)
		hash := parts[1]
		blockSec, _ := strconv.ParseInt(parts[2], 10, 64)
		blockTime := time.Unix(blockSec, 0)
		d.height[uint32(height)] = blockTime
		d.hash[hash] = uint32(height)
	}
	return nil
}
