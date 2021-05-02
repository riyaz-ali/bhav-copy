package main

import (
	"github.com/rs/zerolog/log"
	"go.riyazali.net/bhav/pipeline"
	"sync"
	"time"
)

var day = time.Hour * 24

var holidays = [][2]int{
	{1, 1},   // new year
	{1, 26},  // republic day
	{1, 30},  // gandhi memory day
	{4, 14},  // regional new year
	{5, 1},   // may day
	{8, 15},  // independence day
	{10, 2},  // gandhi jayanthi
	{12, 25}, // christmas
}

func Holiday(d time.Time) bool {
	if w := d.Weekday(); w == time.Saturday || w == time.Sunday { // is a weekend?
		return true
	} else { // falls on a national holiday?
		for _, h := range holidays {
			_, month, day := d.Date()
			if day == h[0] && int(month) == h[1] {
				return true
			}
		}
	}
	return false
}

// EnqueueEquity enqueues job for processing equity data
func EnqueueEquity(from, to time.Time, wg *sync.WaitGroup, exc string, gen func(on time.Time) pipeline.Resource, in chan<- pipeline.Resource) {
	defer wg.Done()
	for d := from; d.Before(to) || d.Equal(to); d = d.Add(day) {
		if Holiday(d) {
			log.Info().Str("exchange", exc).Msgf("skipping job for %s", d.Format("Mon 02 Jan, 2006"))
			continue
		}

		log.Debug().Str("exchange", exc).Msgf("enqueuing job for %s", d.Format("Mon 02 Jan, 2006"))
		in <- gen(d)
	}
}
