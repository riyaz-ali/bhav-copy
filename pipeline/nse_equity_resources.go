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

// NseEquityResource is NSE's bhavcopy resource for the given date
type NseEquityResource struct{ date time.Time }

// NewNseEquity create a new NSE equity resource
func NewNseEquity(on time.Time) Resource { return &NseEquityResource{date: on} }

func (b NseEquityResource) String() string {
	var endpoint = "https://www1.nseindia.com/content/historical/EQUITIES/%s/%s/cm%sbhav.csv.zip"
	return fmt.Sprintf(endpoint, b.date.Format("2006"), uc(b.date.Format("Jan")), uc(b.date.Format("02Jan2006")))
}

func (b NseEquityResource) Fetch() (_ Parseable, err error) {
	var endpoint = "https://www1.nseindia.com/content/historical/EQUITIES/%s/%s/cm%sbhav.csv.zip"
	endpoint = fmt.Sprintf(endpoint, b.date.Format("2006"), uc(b.date.Format("Jan")), uc(b.date.Format("02Jan2006")))

	var request, _ = http.NewRequest(http.MethodGet, endpoint, nil)
	request.Header.Set("Referer", "https://www1.nseindia.com/products/content/equities/equities/archieve_eq.htm")

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

	var fileName = fmt.Sprintf("cm%sbhav.csv", uc(b.date.Format("02Jan2006")))
	var file fs.File
	if file, err = zipReader.Open(fileName); err != nil {
		return nil, errors.Wrapf(err, "failed to open file %s", fileName)
	}

	var data []byte
	if data, err = ioutil.ReadAll(file); err != nil {
		return nil, errors.Wrapf(err, "failed to read from zip file")
	}

	return nseEquityData{data: data}, nil
}

type nseEquityData struct{ data []byte }

func (b nseEquityData) Parse() (_ []Equity, err error) {
	var equities []Equity

	var decoder *csv.Decoder
	if decoder, err = csv.NewDecoder(scsv.NewReader(bytes.NewReader(b.data))); err != nil {
		return nil, err
	}

	for {
		var eq = &NseEquity{}
		if err = decoder.Decode(&eq); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		equities = append(equities, eq)
	}

	return equities, nil
}

// NseEquity implements the Equity interface for BSE's equity data
type NseEquity struct {
	Symbol string  `csv:"SYMBOL"`
	Series string  `csv:"SERIES"`
	Date   csvDate `csv:"TIMESTAMP"`
	Isin   string  `csv:"ISIN,omitempty"`
	Ohlc   struct {
		Open  float64 `csv:"OPEN"`
		High  float64 `csv:"HIGH"`
		Low   float64 `csv:"LOW"`
		Close float64 `csv:"CLOSE"`
	} `csv:",inline"`
	LastValue      float64 `csv:"LAST"`
	PrevCloseValue float64 `csv:"PREVCLOSE"`
}

func (n *NseEquity) Exchange() string       { return "nse" }
func (n *NseEquity) TradingDate() time.Time { return n.Date.Time }
func (n *NseEquity) Ticker() string         { return n.Symbol }
func (n *NseEquity) Type() string           { return n.Series }
func (n *NseEquity) ISIN() string           { return n.Isin }
func (n *NseEquity) Last() float64          { return n.LastValue }
func (n *NseEquity) PrevClose() float64     { return n.PrevCloseValue }
func (n *NseEquity) OHLC() (open, high, low, close float64) {
	return n.Ohlc.Open, n.Ohlc.High, n.Ohlc.Low, n.Ohlc.Close
}
