package converter

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"
)

type blockDate struct {
	data map[uint32]time.Time
}

func newBlockDate() *blockDate {
	return &blockDate{map[uint32]time.Time{}}
}

func (d *blockDate) getDate(height uint32) (time.Time, bool) {
	date, ok := d.data[height]
	return date, ok
}

func (d *blockDate) build(dateFile string) error {
	file, err := os.Open(dateFile)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ",")
		height, _ := strconv.ParseUint(parts[0], 10, 32)
		blockSec, _ := strconv.ParseInt(parts[1], 10, 64)
		blockTime := time.Unix(blockSec, 0)
		d.data[uint32(height)] = blockTime
	}
	return nil
}
