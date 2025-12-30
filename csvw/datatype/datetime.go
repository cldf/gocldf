package datatype

import (
	"errors"
	"fmt"
	"time"
)

var (
	ISO8061Layout = "2006-01-02T15:04:05"
	dateFormat    = map[string]string{
		"yyyy-MM-dd": "2006-01-02",
		"yyyyMMdd":   "20060102",
		"dd-MM-yyyy": "02-01-2006",
		"d-M-yyyy":   "02-1-2006",
		"MM-dd-yyyy": "01-02-2006",
		"M-d-yyyy":   "1-02-2006",
		"dd/MM/yyyy": "02/01/2006",
		"d/M/yyyy":   "02/1/2006",
		"MM/dd/yyyy": "01/02/2006",
		"M/d/yyyy":   "1/02/2006",
		"dd.MM.yyyy": "02.01.2006",
		"d.M.yyyy":   "02.3.2006",
		"MM.dd.yyyy": "01.02.2006",
		"M.d.yyyy":   "1.02.2006",
	}
	timeFormat = map[string]string{
		"HH:mm:ss.S":   "15:04:05.9",
		"HH:mm:ss.SS":  "15:04:05.99",
		"HH:mm:ss.SSS": "15:04:05.999",
		"HH:mm:ss":     "15:04:05",
		"HHmmss":       "150405",
		"HH:mm":        "15:04",
		"HHmm":         "1504",
	}
	timezoneFormat = map[string]string{
		"X":   "Z0700",  // or Z (minutes are optional)
		"XX":  "Z0700",  // or Z
		"XXX": "Z07:00", // or Z
		"x":   "-0700",  // (Z is not permitted)
		"xx":  "-0700",  // (Z is not permitted)
		"xxx": "-07:00", // (Z is not permitted)
	}
	datetimeFormat = map[string]string{
		//with one or more trailing S characters indicating the maximum number of fractional seconds e.g., yyyy-MM-ddTHH:mm:ss.SSS for 2006-01-15T15:02:37.143
		"yyyy-MM-ddTHH:mm:ss.S":   "2006-01-02T15:04:05.9",
		"yyyy-MM-ddTHH:mm:ss.SS":  "2006-01-02T15:04:05.99",
		"yyyy-MM-ddTHH:mm:ss.SSS": "2006-01-02T15:04:05.999",
		"yyyy-MM-ddTHH:mm:ss":     "2006-01-02T15:04:05",
		"yyyy-MM-ddTHH:mm":        "2006-01-02T15:04",
	}
)

func init() {
	for df, dex := range dateFormat {
		for tf, tex := range timeFormat {
			datetimeFormat[df+" "+tf] = dex + " " + tex
		}
	}
	keys := make([]string, 0, len(datetimeFormat))
	for k := range datetimeFormat {
		keys = append(keys, k)
	}
	for _, dtf := range keys {
		for tzf, tzex := range timezoneFormat {
			datetimeFormat[dtf+tzf] = datetimeFormat[dtf] + tzex
			datetimeFormat[dtf+" "+tzf] = datetimeFormat[dtf] + " " + tzex
		}
	}
	// FIXME: add timezone marker as optional to all date/time formats,
	// separated by optional space.
}

var Datetime = baseType{
	getDerivedDescription: func(dtProps map[string]any) (map[string]any, error) {
		val, ok := dtProps["format"]
		if ok {
			s, ok := datetimeFormat[val.(string)]
			if ok {
				return map[string]any{"layout": s}, nil
			}
			fmt.Println(datetimeFormat)
			return map[string]any{}, errors.New(fmt.Sprintf("Unsupported datetime format: %v", val.(string)))
		}
		return map[string]any{"layout": ISO8061Layout}, nil
	},
	setValueConstraints: func(m map[string]stringAndAny) (err error) {
		for k, v := range m {
			if v.str != "" {
				v.val, err = time.Parse(ISO8061Layout, v.str)
			}
			m[k] = v
		}
		return
	},
	toGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
		val, err := time.Parse(dt.DerivedDescription["layout"].(string), s)
		if err != nil {
			return nil, err
		}
		if !noChecks {
			if dt.MinInclusive != nil && val.Before(dt.MinInclusive.(time.Time)) {
				return nil, errors.New("value smaller than minimum")
			}
			if dt.MaxInclusive != nil && val.After(dt.MaxInclusive.(time.Time)) {
				return nil, errors.New("value greater than maximum")
			}
			if dt.MinExclusive != nil && (val.Equal(dt.MaxInclusive.(time.Time)) || val.Before(dt.MaxInclusive.(time.Time))) {
				return nil, errors.New("value smaller than exclusive minimum")
			}
			if dt.MaxExclusive != nil && (val.Equal(dt.MaxInclusive.(time.Time)) || val.After(dt.MaxInclusive.(time.Time))) {
				return nil, errors.New("value greater than exclusive maximum")
			}
		}
		return val, nil
	},
	toString: func(dt *Datatype, x any) (string, error) {
		return x.(time.Time).Format(dt.DerivedDescription["layout"].(string)), nil
	},
	sqlType: "TEXT",
	toSql: func(dt *Datatype, x any) (any, error) {
		return x.(time.Time).Format(dt.DerivedDescription["layout"].(string)), nil
	},
}
