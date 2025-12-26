package datatype

import (
	"errors"
	"regexp"
)

var String = BaseType{
	GetDerivedDescription: func(dtProps map[string]any) (map[string]any, error) {
		val, ok := dtProps["format"]
		if ok {
			// FIXME: must make sure regex is anchored on both sides! I.e. wrap in "^$" if necessary.
			return map[string]any{"regex": regexp.MustCompile(val.(string))}, nil
		}
		return map[string]any{"regex": nil}, nil
	},
	ToGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
		if !noChecks {
			if dt.Length != -1 && len(s) != dt.Length {
				return nil, errors.New("invalid length")
			}
			if dt.MinLength != -1 && len(s) < dt.MinLength {
				return nil, errors.New("invalid length")
			}
			if dt.MaxLength != -1 && len(s) > dt.MaxLength {
				return nil, errors.New("invalid length")
			}
			if dt.DerivedDescription["regex"] != nil {
				if !dt.DerivedDescription["regex"].(*regexp.Regexp).MatchString(s) {
					return nil, errors.New("invalid value")
				}
			}
		}
		return s, nil
	},
	ToString: func(dt *Datatype, x any) (string, error) {
		return x.(string), nil
	},
	SqlType: "TEXT",
	ToSql:   func(dt *Datatype, x any) (any, error) { return x.(string), nil },
}
