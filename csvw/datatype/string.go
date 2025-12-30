package datatype

import (
	"errors"
	"regexp"
	"strings"
)

var String = baseType{
	getDerivedDescription: func(dtProps map[string]any, m map[string]stringAndAny) (map[string]any, error) {
		val, ok := dtProps["format"]
		if ok {
			fmt, ok := val.(string)
			if !ok {
				return nil, errors.New("format property must be a string")
			}
			if !strings.HasPrefix(fmt, "^") {
				fmt = "^" + fmt
			}
			if !strings.HasSuffix(fmt, "$") {
				fmt += "$"
			}
			regex, err := regexp.Compile(fmt)
			if err != nil {
				return nil, err
			}
			return map[string]any{"regex": regex}, nil
		}
		return map[string]any{"regex": nil}, nil
	},
	toGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
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
	toString: func(dt *Datatype, x any) (string, error) {
		return x.(string), nil
	},
	sqlType: "TEXT",
	toSql:   func(dt *Datatype, x any) (any, error) { return x.(string), nil },
}
