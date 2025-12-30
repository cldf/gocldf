package datatype

import (
	"errors"
	"fmt"
	"strings"
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
		// with one or more trailing S characters indicating the maximum number of fractional seconds e.g.,
		// yyyy-MM-ddTHH:mm:ss.SSS for 2006-01-15T15:02:37.143
		"yyyy-MM-ddTHH:mm:ss.S":   "2006-01-02T15:04:05.9",
		"yyyy-MM-ddTHH:mm:ss.SS":  "2006-01-02T15:04:05.99",
		"yyyy-MM-ddTHH:mm:ss.SSS": "2006-01-02T15:04:05.999",
		"yyyy-MM-ddTHH:mm:ss":     "2006-01-02T15:04:05",
		"yyyy-MM-ddTHH:mm":        "2006-01-02T15:04",
	}
)

var appendTimeZoneFormats = func(formats map[string]string) {
	keys := make([]string, 0, len(formats))
	for k := range formats {
		keys = append(keys, k)
	}
	for _, dtf := range keys {
		for tzf, tzex := range timezoneFormat {
			formats[dtf+tzf] = formats[dtf] + tzex
			formats[dtf+" "+tzf] = formats[dtf] + " " + tzex
		}
	}
}

func init() {
	// "any of the date formats above, followed by a single space, followed by any of the time formats above"
	for df, dex := range dateFormat {
		for tf, tex := range timeFormat {
			datetimeFormat[df+" "+tf] = dex + " " + tex
		}
	}
	appendTimeZoneFormats(datetimeFormat)
	appendTimeZoneFormats(dateFormat)
	appendTimeZoneFormats(timeFormat)
}

func toGo(dt *Datatype, s string, noChecks bool) (any, error) {
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
		if dt.MinExclusive != nil && (val.Equal(dt.MinExclusive.(time.Time)) || val.Before(dt.MinExclusive.(time.Time))) {
			return nil, errors.New("value smaller than exclusive minimum")
		}
		if dt.MaxExclusive != nil && (val.Equal(dt.MaxExclusive.(time.Time)) || val.After(dt.MaxExclusive.(time.Time))) {
			return nil, errors.New("value greater than exclusive maximum")
		}
	}
	return val, nil
}

func toString(dt *Datatype, x any) (string, error) {
	return x.(time.Time).Format(dt.DerivedDescription["layout"].(string)), nil
}
func toSql(dt *Datatype, x any) (any, error) {
	return x.(time.Time).Format(dt.DerivedDescription["layout"].(string)), nil
}

var dateTime = baseType{
	getDerivedDescription: func(dtProps map[string]any, m map[string]stringAndAny) (res map[string]any, err error) {
		res = make(map[string]any)
		res["layout"] = ISO8061Layout
		val, ok := dtProps["format"]
		if ok {
			s, ok := datetimeFormat[val.(string)]
			if ok {
				res["layout"] = s
			} else {
				return map[string]any{}, errors.New(fmt.Sprintf("Unsupported datetime format: %v", val.(string)))
			}
		}
		for k, v := range m {
			if v.str != "" {
				v.val, err = time.Parse(res["layout"].(string), v.str)
			}
			m[k] = v
		}
		return res, nil
	},
	toGo:     toGo,
	toString: toString,
	sqlType:  "TEXT",
	toSql:    toSql,
}

var dateTimeStamp = baseType{
	getDerivedDescription: func(dtProps map[string]any, m map[string]stringAndAny) (res map[string]any, err error) {
		res = make(map[string]any)
		res["layout"] = ISO8061Layout + "Z07:00"
		val, ok := dtProps["format"]
		if ok {
			s, ok := val.(string)
			if !ok {
				return map[string]any{}, errors.New("dateTimeStamp format must be string")
			}
			s, ok = datetimeFormat[s]
			if ok {
				if strings.HasSuffix(s, "x") || strings.HasSuffix(s, "X") {
					res["layout"] = s
				} else {
					return map[string]any{}, errors.New("datetimeStamp format must have explicit timezone")
				}
			} else {
				return map[string]any{}, errors.New(fmt.Sprintf("Unsupported datetime format: %v", val.(string)))
			}
		}
		for k, v := range m {
			if v.str != "" {
				v.val, err = time.Parse(res["layout"].(string), v.str)
			}
			m[k] = v
		}
		return res, nil
	},
	toGo:     toGo,
	toString: toString,
	sqlType:  "TEXT",
	toSql:    toSql,
}

var date = baseType{
	getDerivedDescription: func(dtProps map[string]any, m map[string]stringAndAny) (res map[string]any, err error) {
		res = make(map[string]any)
		res["layout"], _, _ = strings.Cut(ISO8061Layout, "T")
		val, ok := dtProps["format"]
		if ok {
			s, ok := dateFormat[val.(string)]
			if ok {
				res["layout"] = s
			} else {
				return map[string]any{}, errors.New(fmt.Sprintf("Unsupported date format: %v", val.(string)))
			}
		}
		for k, v := range m {
			if v.str != "" {
				v.val, err = time.Parse(res["layout"].(string), v.str)
			}
			m[k] = v
		}
		return res, nil
	},
	toGo:     toGo,
	toString: toString,
	sqlType:  "TEXT",
	toSql:    toSql,
}

var Time = baseType{
	getDerivedDescription: func(dtProps map[string]any, m map[string]stringAndAny) (res map[string]any, err error) {
		res = make(map[string]any)
		_, res["layout"], _ = strings.Cut(ISO8061Layout, "T")
		val, ok := dtProps["format"]
		if ok {
			s, ok := timeFormat[val.(string)]
			if ok {
				res["layout"] = s
			} else {
				return map[string]any{}, errors.New(fmt.Sprintf("Unsupported date format: %v", val.(string)))
			}
		}
		for k, v := range m {
			if v.str != "" {
				v.val, err = time.Parse(res["layout"].(string), v.str)
			}
			m[k] = v
		}
		return res, nil
	},
	toGo:     toGo,
	toString: toString,
	sqlType:  "TEXT",
	toSql:    toSql,
}
