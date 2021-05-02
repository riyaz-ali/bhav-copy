package pipeline

import (
	"archive/zip"
	"bytes"
	scsv "encoding/csv"
	"fmt"
	csv "github.com/jszwec/csvutil"
	"github.com/pkg/errors"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"time"
)

// BseEquityResource is BSE's bhavcopy resource for the given date
type BseEquityResource struct{ date time.Time }

// NewBseEquity create a new BSE equity resource
func NewBseEquity(on time.Time) Resource { return &BseEquityResource{date: on} }

func (b *BseEquityResource) String() string {
	var endpoint = "https://www.bseindia.com/download/BhavCopy/Equity/EQ%s_csv.zip"
	return fmt.Sprintf(endpoint, b.date.Format("020106"))
}

func (b *BseEquityResource) Fetch() (_ Parseable, err error) {
	var endpoint = "https://www.bseindia.com/download/BhavCopy/Equity/EQ%s_csv.zip"
	endpoint = fmt.Sprintf(endpoint, b.date.Format("020106"))

	var request, _ = http.NewRequest(http.MethodGet, endpoint, nil)
	var response *http.Response
	if response, err = Client.Do(request); err != nil {
		return nil, errors.Wrapf(err, "failed to fetch %q", endpoint)
	} else if status := response.StatusCode; status != 200 {
		return nil, errors.Errorf("server returned %d", status)
	}

	var size int64
	var buf bytes.Buffer // zip needs to be seek-able; read everything in memory!
	if size, err = buf.ReadFrom(response.Body); err != nil {
		return nil, errors.Wrapf(err, "failed to read response from %s", endpoint)
	}

	var zipReader *zip.Reader
	if zipReader, err = zip.NewReader(bytes.NewReader(buf.Bytes()), size); err != nil {
		return nil, errors.Wrapf(err, "failed to unzip response")
	}

	var fileName = fmt.Sprintf("EQ%s.CSV", b.date.Format("020106"))
	var file fs.File
	if file, err = zipReader.Open(fileName); err != nil {
		return nil, errors.Wrapf(err, "failed to open file %s", fileName)
	}

	var data []byte
	if data, err = ioutil.ReadAll(file); err != nil {
		return nil, errors.Wrapf(err, "failed to read from zip file")
	}

	return bseEquityData{data: data, date: b.date}, nil
}

type bseEquityData struct {
	data []byte
	date time.Time // bse reports don't contain time information
}

func (b bseEquityData) Parse() (_ []Equity, err error) {
	var equities []Equity

	var decoder *csv.Decoder
	if decoder, err = csv.NewDecoder(scsv.NewReader(bytes.NewReader(b.data))); err != nil {
		return nil, err
	}

	for {
		var eq = &BseEquity{Date: b.date}
		if err = decoder.Decode(&eq); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		equities = append(equities, eq)
	}

	return equities, nil
}

// BseEquity implements the Equity interface for BSE's equity data
type BseEquity struct {
	Code      string `csv:"SC_CODE"`
	Date      time.Time
	Isin      string `csv:"ISIN_CODE,omitempty"`
	ScripType string `csv:"SC_TYPE"`
	Ohlc      struct {
		Open  float64 `csv:"OPEN"`
		High  float64 `csv:"HIGH"`
		Low   float64 `csv:"LOW"`
		Close float64 `csv:"CLOSE"`
	} `csv:",inline"`
	LastValue      float64 `csv:"LAST"`
	PrevCloseValue float64 `csv:"PREVCLOSE"`
}

func (_ *BseEquity) Exchange() string       { return "bse" }
func (b *BseEquity) TradingDate() time.Time { return b.Date }
func (b *BseEquity) Ticker() string         { return defaultsTo(bseLookup(b.Code).SecurityId, b.Code) }
func (b *BseEquity) Type() string           { return b.ScripType }
func (b *BseEquity) ISIN() string           { return defaultsTo(b.Isin, bseLookup(b.Code).ISIN) }
func (b *BseEquity) Last() float64          { return b.LastValue }
func (b *BseEquity) PrevClose() float64     { return b.PrevCloseValue }
func (b *BseEquity) OHLC() (open, high, low, close float64) {
	return b.Ohlc.Open, b.Ohlc.High, b.Ohlc.Low, b.Ohlc.Close
}
