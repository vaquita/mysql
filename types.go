/*
  The MIT License (MIT)

  Copyright (c) 2015 Nirbhay Choubey

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/

package mysql

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	// MaxTime is the maximum allowable TIMESTAMP value in MySQL '2038-01-19 03:14:07 UTC'
	MaxTime time.Time = time.Date(2038, time.January, 19, 3, 14, 7, 0, time.UTC)
	// MinTime is the minimum allowable TIMESTAMP value in MySQL '1970-01-01 00:00:01' UTC'
	MinTime time.Time = time.Date(1970, time.January, 01, 0, 0, 1, 0, time.UTC)

	// MaxDuration is the maximum allowable TIME value in MySQL '838:59:59.000000'
	MaxDuration time.Duration = 838*time.Hour + 59*time.Minute + 59*time.Second
	// MinDuration is the minimun allowable TIME value in MySQL '-838:59:59.000000'
	MinDuration time.Duration = -1 * MaxDuration

	InvalidTime     time.Time     = MaxTime.Add(1 * time.Second)
	InvalidDuration time.Duration = MaxDuration + 1*time.Second
)

// NullTime represents a Time type the may be null.
type NullTime struct {
	Time  time.Time
	Valid bool
}

// Scan implements the scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	if value == nil {
		nt.Time, nt.Valid = InvalidTime, false
		return nil
	}

	switch v := value.(type) {
	case time.Time:
		nt.Time, nt.Valid = v, true
		return nil

	// TODO: handle other types/cases
	default:
	}
	return nil
}

// Value implements the driver's Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

type NullDuration struct {
	Duration time.Duration
	Valid    bool
}

// Scan implements the scanner interface.
func (nd *NullDuration) Scan(value interface{}) error {
	var err error

	if value == nil {
		nd.Duration, nd.Valid = InvalidDuration, false
		return nil
	}

	switch v := value.(type) {
	case string:
		if nd.Duration, err = parseDuration(v); err != nil {
			nd.Duration, nd.Valid = 0, false
			return nil
		}
		nd.Valid = true

	case time.Duration:
		nd.Duration, nd.Valid = v, true

	// TODO: handle other types/cases
	default:
	}

	return nil
}

// Value implements the driver's Valuer interface.
func (nd NullDuration) Value() (driver.Value, error) {
	if !nd.Valid {
		return nil, nil
	}
	return formatDuration(nd.Duration), nil
}

// parseDuration parses the input specified in MySQL's TIME format into
// mysql.Duration type.
func parseDuration(s string) (time.Duration, error) {
	var d time.Duration

	v := strings.Split(s, ":")
	switch len(v) {
	case 3:
		if secs, err := strconv.ParseFloat(v[2], 64); err != nil {
			return 0, myError(ErrInvalidType, err)
		} else {
			d += time.Duration(secs*1000000) * time.Microsecond
		}
		fallthrough
	case 2:
		if mins, err := strconv.ParseInt(v[1], 10, 64); err != nil {
			return 0, myError(ErrInvalidType, err)
		} else {
			d += time.Duration(mins) * time.Minute
		}
		fallthrough
	case 1:
		if hours, err := strconv.ParseInt(v[0], 10, 64); err != nil {
			return 0, myError(ErrInvalidType, err)
		} else {
			d += time.Duration(hours) * time.Hour
		}
	default:
	}

	return d, nil
}

// formatDuration formats the specified time.Duration in MySQL TIME format.
func formatDuration(d time.Duration) string {
	var neg string

	if d < 0 {
		neg = "-"
		d *= -1
	}

	hours := int(d / time.Hour)
	d %= time.Hour

	mins := int(d / time.Minute)
	d %= time.Minute

	secs := (float64(d/time.Microsecond) / 1000000)

	if secs == 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d", neg, hours, mins, 0)
	}
	return fmt.Sprintf("%s%02d:%02d:%02f", neg, hours, mins, secs)
}

// for internal use only
type nullString struct {
	value string
	valid bool // valid is true if 'the string' is not NULL
}

var defaultParameterConverter DefaultParameterConverter

type DefaultParameterConverter struct{}

func (DefaultParameterConverter) ConvertValue(v interface{}) (driver.Value, error) {
	switch s := v.(type) {
	case NullTime:
		if s.Valid == false {
			return nil, nil
		} else {
			return s.Time, nil
		}
	case time.Duration:
		return formatDuration(s), nil
	case NullDuration:
		if s.Valid == false {
			return nil, nil
		} else {
			return formatDuration(s.Duration), nil
		}
	default:
		return driver.DefaultParameterConverter.ConvertValue(v)
	}
	// shouldn't reach here
	return nil, nil
}
