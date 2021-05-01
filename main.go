package main

import (
	"crawshaw.io/sqlite"
	"flag"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.riyazali.net/bhav/schema"
	"os"
)

// flags used to open sqlite database
const flags = sqlite.SQLITE_OPEN_CREATE | sqlite.SQLITE_OPEN_READWRITE

func init() {
	// set the default package-level logger
	log.Logger = zerolog.New(zerolog.NewConsoleWriter())
}

func main() {
	var err error
	var filename = flag.String("sync", "bhav.db", "database file to sync to")
	var savePatch = flag.Bool("save-patch", false, "write the changeset to a patch file")
	flag.Parse()

	// open a connection and start a session to record changes to the dataset
	log.Info().Str("file", *filename).Msg("opening database file")
	var conn *sqlite.Conn
	if conn, err = sqlite.OpenConn(*filename, flags); err != nil {
		log.Fatal().Err(err).Send()
	}

	var session *sqlite.Session
	if session, err = conn.CreateSession("main"); err != nil {
		log.Fatal().Err(err).Send()
	}

	if err = session.Attach(""); err != nil { // attach to all tables
		log.Fatal().Err(err).Send()
	}

	log.Info().Msgf("applying schema migration to %s", filename)
	if err := schema.Apply(conn); err != nil {
		log.Fatal().Err(err).Send()
	}
	session.Enable()

	// TODO: add magic here!

	session.Disable()

	if *savePatch { // should save patch?
		var patchFileName = fmt.Sprintf("%s.patch", *filename)
		log.Info().Str("file", patchFileName).Msg("writing patch to file")
		var file *os.File
		if file, err = os.Create(patchFileName); err != nil {
			log.Fatal().Err(err).Send()
		}

		if err = session.Changeset(file); err != nil {
			log.Fatal().Err(err).Send()
		}

		_ = file.Close()
	}

	session.Delete() // close the session
	_ = conn.Close()
}
