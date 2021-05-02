package main

import (
	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	_ "embed"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	flag "github.com/spf13/pflag"
	"go.riyazali.net/bhav/pipeline"
	"go.riyazali.net/bhav/schema"
	"os"
	"sync"
	"time"
)

//go:embed queries/insert_equity.sql
var insertIntoEquity string // query to insert data into "equity" table

//go:embed queries/last_trading_date_by_exchange.sql
var lastTradingDate string // query to fetch last trading date by exchange

// minimum dates for bse and nse
var (
	BseMinimumDate = time.Date(2007, 01, 01, 0, 0, 0, 0, time.FixedZone("IST", 0530))
	NseMinimumDate = time.Date(1994, 11, 03, 0, 0, 0, 0, time.FixedZone("IST", 0530))
)

// flags used by the tool
var filename string          // database file name
var savePatch bool           // should write patch file?
var fromDate date            // date to start syncing from
var until = date(time.Now()) // hidden flag to set the end date for sync; default to today
var verbose bool             // set to verbose logging

func init() {
	// set the default package-level logger
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		With().Timestamp().Logger()

	// configure flags
	flag.StringVar(&filename, "filename", "bhavcopy.db", "database file to sync")
	flag.BoolVar(&savePatch, "save-patch", false, "save changeset to a patch file")
	flag.Var(&fromDate, "from", "date to start syncing from")
	flag.BoolVar(&verbose, "verbose", false, "enable verbose logging")

	flag.Var(&until, "until", "date to sync until")
	_ = flag.CommandLine.MarkHidden("until")
}

func main() {
	var err error
	flag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// open a connection and start a session to record changes to the dataset
	log.Info().Str("file", filename).Msg("opening database file")
	var conn *sqlite.Conn
	const flags = sqlite.SQLITE_OPEN_CREATE | sqlite.SQLITE_OPEN_READWRITE
	if conn, err = sqlite.OpenConn(filename, flags); err != nil {
		log.Fatal().Err(err).Msg("failed to open database file")
	}

	var session *sqlite.Session
	if session, err = conn.CreateSession("main"); err != nil {
		log.Fatal().Err(err).Msg("failed to start sqlite session")
	}

	if err = session.Attach(""); err != nil { // attach to all tables
		log.Fatal().Err(err).Msg("failed to attach tables to session")
	}

	log.Info().Msgf("applying schema migration to %s", filename)
	if err := schema.Apply(conn); err != nil {
		log.Fatal().Err(err).Msg("failed to apply migration")
	}

	log.Info().Msg("computing time delta")
	// figure out start date; end date is always today
	var end = time.Time(until)
	var bseLast, nseLast = minDatabaseDate(conn) // last trading day recorded in the database

	var bseStart = closest(end, BseMinimumDate, time.Time(fromDate), bseLast.Add(time.Hour*24))
	var nseStart = closest(end, NseMinimumDate, time.Time(fromDate), nseLast.Add(time.Hour*24))
	log.Debug().Time("bse", bseStart).Time("nse", nseStart).Time("end", end).Msg("computed time delta")

	// create a background pipeline to process equity data
	var in, out = pipeline.EquityPipeline()
	var ins = conn.Prep(insertIntoEquity)

	if bseStart.After(end) && nseStart.After(end) { // no data to fetch
		log.Info().Msg("everything is in sync")
		goto end
	}

	{ // start background enqueue tasks to push resources into input channel
		// use WaitGroup to close input once we're done enqueuing
		log.Debug().Msg("starting enqueue process")
		var wg sync.WaitGroup
		wg.Add(2)
		go EnqueueEquity(bseStart, end, &wg, "bse", pipeline.NewBseEquity, in)
		go EnqueueEquity(nseStart, end, &wg, "nse", pipeline.NewNseEquity, in)
		go func() { wg.Wait(); close(in) }()
	}

	log.Debug().Msg("enabling sqlite session")
	session.Enable()
	{
		// range over output and insert records into database
		for eqs := range out {
			_ = sqlitex.Exec(conn, "BEGIN", nil)
			for _, eq := range eqs {
				ins.SetText(":exchange", eq.Exchange())
				ins.SetText(":trading_date", eq.TradingDate().Format("2006-01-02"))
				ins.SetText(":ticker", eq.Ticker())
				ins.SetText(":type", eq.Type())
				ins.SetText(":isin_code", eq.ISIN())

				var o, h, l, c = eq.OHLC()
				ins.SetFloat(":open", o)
				ins.SetFloat(":high", h)
				ins.SetFloat(":low", l)
				ins.SetFloat(":close", c)

				ins.SetFloat(":last", eq.Last())
				ins.SetFloat(":previous_close", eq.PrevClose())

				if _, err = ins.Step(); err != nil {
					log.Warn().Err(err).Msg("failed to insert row")
				}
				_ = ins.Reset()
			}
			_ = sqlitex.Exec(conn, "COMMIT", nil)
		}
	}
	log.Debug().Msg("disabling sqlite session")
	session.Disable()

	if savePatch { // should save patch?
		var patchFileName = fmt.Sprintf("%s.patch", filename)
		log.Debug().Str("file", patchFileName).Msg("writing patch to file")
		var file *os.File
		if file, err = os.Create(patchFileName); err != nil {
			log.Fatal().Err(err).Send()
		}

		if err = session.Changeset(file); err != nil {
			log.Fatal().Err(err).Send()
		}

		_ = file.Close()
		log.Info().Str("filename", patchFileName).Msg("changeset written to patch file")
	}

end:

	session.Delete() // close the session
	_ = conn.Close()
}
