package main

import (
	"crawshaw.io/sqlite"
	"github.com/rs/zerolog/log"
	"math"
	"time"
)

// date implements pflag.Value to parse timestamp from command-line
type date time.Time

func (d *date) String() string     { return time.Time(*d).Format("02-Jan-2006") }
func (d *date) Type() string       { return "timestamp" }
func (d *date) Set(s string) error { tt, err := time.Parse("02-Jan-2006", s); *d = date(tt); return err }

// returns last sync date from database
func minDatabaseDate(c *sqlite.Conn) (bse, nse time.Time) {
	var stmt = c.Prep(lastTradingDate)
	defer stmt.Finalize()

	for {
		if r, err := stmt.Step(); err != nil {
			log.Fatal().Err(err).Msg("failed to fetch sync information from database")
		} else if !r {
			break
		}

		switch ex := stmt.GetText("exchange"); ex {
		case "bse":
			bse, _ = time.Parse("2006-01-02", stmt.GetText("last_trading_date"))
		case "nse":
			nse, _ = time.Parse("2006-01-02", stmt.GetText("last_trading_date"))
		default:
			log.Fatal().Msgf("unknown exchange: %s", ex)
		}
	}
	return bse, nse
}

func closest(to time.Time, values ...time.Time) time.Time {
	var c time.Duration = math.MaxInt64 // infinitely far
	for _, val := range values {
		d := to.Sub(val)
		if d < c {
			c = d
		}
	}
	return to.Add(-c)
}

