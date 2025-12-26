package datatype

import (
	"errors"
	"time"
)

var (
	ISO8061Layout  = "2006-01-02T15:04:05"
	FormatToLayout = map[string]string{
		//with one or more trailing S characters indicating the maximum number of fractional seconds e.g., yyyy-MM-ddTHH:mm:ss.SSS for 2015-03-15T15:02:37.143
		"yyyy-MM-ddTHH:mm:ss.S": "2015-03-15T15:02:37.1",
		"yyyy-MM-ddTHH:mm:ss":   "2015-03-15T15:02:37",
		"yyyy-MM-ddTHH:mm":      "2015-03-15T15:02",
		// any of the date formats above, followed by a single space,
		// followed by any of the time formats above, e.g., M/d/yyyy HH:mm
		// for 3/22/2015 15:02 or dd.MM.yyyy HH:mm:ss for 22.03.2015 15:02:37
	}
)

var Datetime = baseType{
	GetDerivedDescription: func(dtProps map[string]any) (map[string]any, error) {
		val, ok := dtProps["format"]
		if ok {
			// FIXME: must make sure regex is anchored on both sides! I.e. wrap in "^$" if necessary.
			return map[string]any{"layout": FormatToLayout[val.(string)]}, nil
		}
		return map[string]any{"layout": ISO8061Layout}, nil
	},
	SetValueConstraints: func(m map[string]stringAndAny) (err error) {
		for k, v := range m {
			if v.str != "" {
				v.val, err = time.Parse(ISO8061Layout, v.str)
			}
			m[k] = v
		}
		return
	},
	ToGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
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
	ToString: func(dt *Datatype, x any) (string, error) {
		return x.(time.Time).Format(dt.DerivedDescription["layout"].(string)), nil
	},
	SqlType: "TEXT",
	ToSql: func(dt *Datatype, x any) (any, error) {
		return x.(time.Time).Format(dt.DerivedDescription["layout"].(string)), nil
	},
}
