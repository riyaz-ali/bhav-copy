package pipeline

import (
	"strings"
	"time"
)

var uc = strings.ToUpper

// helper to deal with data format in reports
type csvDate struct{ time.Time }

func (b *csvDate) UnmarshalCSV(data []byte) error {
	if tt, err := time.Parse("2-Jan-2006",string(data)); err != nil {
		return err
	} else {
		*b = csvDate{Time: tt}
		return nil
	}
}
