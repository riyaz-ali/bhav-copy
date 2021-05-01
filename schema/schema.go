// Package schema provides the sqlite schema migrations and utility functions to apply those.
package schema

import (
	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"embed"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"io/fs"
	"io/ioutil"
)

//go:embed *.sql
var migrations embed.FS // embedded migration scripts

// Apply applies all the pending schema migrations to the primary database
// in the provided sqlite connection. It increments the user_version and set
// it to the latest value for the last migration that was executed.
func Apply(c *sqlite.Conn) (err error) {
	defer sqlitex.Save(c)(&err) // migrations are transactional!

	var getVersion = func() int64 {
		var v int64
		_ = sqlitex.Exec(c, "PRAGMA user_version",
			func(stmt *sqlite.Stmt) error { v = stmt.GetInt64("user_version"); return nil })
		return v
	}

	var setVersion = func(v int64) error {
		return sqlitex.Exec(c, fmt.Sprintf("PRAGMA user_version = %d", v), nil)
	}


	log.Info().Msgf("current migration version is v%d", getVersion())
	var current int64 = 0 // currently processing migration

	// for each file in the directory, we read+apply it returning error if any
	return fs.WalkDir(migrations, ".", func(path string, entry fs.DirEntry, _ error) error {
		if entry.IsDir() {
			return nil
		}

		_, _ = fmt.Sscanf(entry.Name(), "v%d.sql", &current)
		if current <= getVersion() { // skip this migration if its already applied
			log.Info().Str("file", entry.Name()).Msgf("skipping version v%d", current)
			return nil
		}

		var file, _ = migrations.Open(path)
		var buf, _ = ioutil.ReadAll(file)

		log.Info().Str("file", entry.Name()).Msgf("applying script version v%d", current)
		if e := sqlitex.ExecScript(c, string(buf)); e != nil {
			return errors.Wrapf(e, "failed to apply migration(%s)", entry.Name())
		}

		return errors.Wrapf(setVersion(current), "failed to update version to v%d", current)
	})
}
