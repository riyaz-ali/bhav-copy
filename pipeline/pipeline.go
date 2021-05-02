// Package pipeline provides a background processing pipeline that download resources
// from online repositories, parses them and publishes records using background goroutines.
package pipeline

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// Client is the default http client used by the package
// It's global (and exported) so that we can override this value in tests
var Client = http.DefaultClient

// Equity represents the historical stock / equity related information
// for a given symbol / ticker on a given exchange at a given date.
type Equity interface {
	Exchange() string
	TradingDate() time.Time
	Ticker() string
	Type() string
	ISIN() string
	OHLC() (open, high, low, close float64)
	Last() float64
	PrevClose() float64
}

// Resource represents a network resource that can be fetched and read from.
type Resource interface {
	fmt.Stringer
	Fetch() (Parseable, error)
}

// Parseable represents an in-memory buffer of data that can be parsed into an Equity object
type Parseable interface {
	Parse() ([]Equity, error)
}

// EquityPipeline creates a new background worker pipeline to process equity data
func EquityPipeline() (chan<- Resource, <-chan []Equity) {
	var input = make(chan Resource)

	var downloaders []<-chan Parseable
	for i := 0; i < runtime.NumCPU()*2; i++ {
		downloaders = append(downloaders, downloader(input))
	}

	var dl = mergeDownloaders(downloaders...)
	var parsers []<-chan []Equity
	for i := 0; i < runtime.NumCPU(); i++ {
		parsers = append(parsers, parser(dl))
	}

	return input, mergeParsers(parsers...)
}

func downloader(input <-chan Resource) <-chan Parseable {
	var out = make(chan Parseable)

	go func() {
		for resource := range input {
			log.Info().Str("resource", resource.String()).Msg("downloading resource")
			if r, err := resource.Fetch(); err != nil {
				log.Warn().Err(err).Str("resource", resource.String()).Msg("failed to download resource")
			} else {
				out <- r
			}
		}
		close(out)
	}()

	return out
}

func mergeDownloaders(c ...<-chan Parseable) <-chan Parseable {
	var wg sync.WaitGroup
	var merged = make(chan Parseable)

	// increase counter to number of channels len(c)
	// as we will spawn number of goroutines equal to number of channels received to merge
	wg.Add(len(c))

	// function that accept a channel to push objects to merged channel
	var output = func(pc <-chan Parseable) {
		for p := range pc {
			merged <- p
		}
		wg.Done()
	}

	// run above `output` function as goroutines, `n` number of times
	// where n is equal to number of channels received as argument the function
	for _, ch := range c {
		go output(ch)
	}

	// run goroutine to close merged channel once done
	go func() { wg.Wait(); close(merged) }()

	return merged
}

func parser(input <-chan Parseable) <-chan []Equity {
	var out = make(chan []Equity)

	go func() {
		for r := range input {
			if eq, err := r.Parse(); err != nil {
				log.Warn().Err(err).Msg("failed to parse result")
			} else {
				out <- eq
			}
		}
		close(out)
	}()

	return out
}

func mergeParsers(c ...<-chan []Equity) <-chan []Equity {
	var wg sync.WaitGroup
	var merged = make(chan []Equity)

	// increase counter to number of channels len(c)
	// as we will spawn number of goroutines equal to number of channels received to merge
	wg.Add(len(c))

	// function that accept a channel to push objects to merged channel
	var output = func(pc <-chan []Equity) {
		for p := range pc {
			merged <- p
		}
		wg.Done()
	}

	// run above `output` function as goroutines, `n` number of times
	// where n is equal to number of channels received as argument the function
	for _, ch := range c {
		go output(ch)
	}

	// run goroutine to close merged channel once done
	go func() { wg.Wait(); close(merged) }()

	return merged
}
