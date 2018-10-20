package blockdate

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const BTC_COM_HOME_PAGE = "https://www.blockchain.com/btc/blocks"

type BlockMeta struct {
	ReceivedTime time.Time
	Hash         string
}

type BtcCom struct{}

func (b *BtcCom) LastPage() *BtcComPage {
	return &BtcComPage{
		BTC_COM_HOME_PAGE,
		"",
		map[int64]BlockMeta{},
	}
}

type BtcComPage struct {
	pageUrl     string
	prevPageUrl string
	Blocks      map[int64]BlockMeta
}

func (p *BtcComPage) PrevPage() *BtcComPage {
	if _, ok := p.Blocks[0]; ok {
		return nil
	}
	return &BtcComPage{
		p.prevPageUrl,
		"",
		map[int64]BlockMeta{},
	}
}

func (p *BtcComPage) Fetch() error {
	client := &http.Client{}
	req, err := http.NewRequest("GET", p.pageUrl, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36")
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return errors.New("Response status: " + res.Status)
	}

	doc, err := goquery.NewDocumentFromResponse(res)
	if err != nil {
		return err
	}

	re := regexp.MustCompile(`[^\d]+`)
	doc.Find(".table.table-striped tr").Each(func(_ int, s *goquery.Selection) {
		if strings.TrimSpace(s.Find("td").Eq(0).Text()) == "" {
			return
		}
		heightStr := re.ReplaceAllString(strings.Replace(strings.TrimSpace(s.Find("td").Eq(0).Text()), ",", "", -1), "")
		height, _ := strconv.ParseInt(heightStr, 10, 64)
		timestamp, _ := time.Parse("2006-01-02 15:04:05", s.Find("td").Eq(1).Text())
		hash := s.Find("td").Eq(2).Text()
		p.Blocks[height] = BlockMeta{timestamp, hash}
	})

	prevPageUrl, _ := doc.Find("body > div > h2 > a:nth-child(1)").Attr("href")
	p.prevPageUrl = "https://www.blockchain.com" + prevPageUrl

	return nil
}
