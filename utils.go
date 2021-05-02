package main

import (
	"crawshaw.io/sqlite"
	"github.com/rs/zerolog/log"
	"math"
	"time"
)

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

