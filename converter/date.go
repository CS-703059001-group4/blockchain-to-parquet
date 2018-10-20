package converter

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"
)

type blockDate struct {
	height map[uint32]time.Time
	hash   map[string]uint32
}

func newBlockDate() *blockDate {
	return &blockDate{
		map[uint32]time.Time{},
		map[string]uint32{},
	}
}

func (d *blockDate) getHeight(hash string) (uint32, bool) {
	height, ok := d.hash[hash]
	return height, ok
}

func (d *blockDate) getDate(height uint32) (time.Time, bool) {
	date, ok := d.height[height]
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
		hash := parts[1]
		blockSec, _ := strconv.ParseInt(parts[2], 10, 64)
		blockTime := time.Unix(blockSec, 0)
		d.height[uint32(height)] = blockTime
		d.hash[hash] = uint32(height)
	}
	return nil
}
