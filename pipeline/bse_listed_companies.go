package pipeline

import (
	"bytes"
	_ "embed"
	scsv "encoding/csv"
	csv "github.com/jszwec/csvutil"
	"github.com/rs/zerolog/log"
	"io"
)

//go:embed bse_listed_companies.csv
var listOfListedCompanies []byte

type bseCompany struct {
	ScripCode    string `csv:"Security Code"`
	SecurityId   string `csv:"Security Id"`
	SecurityName string `csv:"Security Name"`
	Status       string `csv:"Status"`
	ISIN         string `csv:"ISIN No"`
}

// map of scrip code to company details populated using csv
var scripCodes = make(map[string]bseCompany)

func bseLookup(code string) bseCompany { return scripCodes[code] }

func init() {
	var err error

	var decoder *csv.Decoder
	if decoder, err = csv.NewDecoder(scsv.NewReader(bytes.NewReader(listOfListedCompanies))); err != nil {
		log.Fatal().Err(err).Msg("failed to read list of listed companies on bse")
	}

	for {
		var company bseCompany
		if err = decoder.Decode(&company); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal().Err(err).Msg("failed to read row from csv")
		}
		scripCodes[company.ScripCode] = company
	}
}
