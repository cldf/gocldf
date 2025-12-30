package datatype

import (
	"errors"
	"fmt"
	"strconv"
)

var decimal = baseType{
	getDerivedDescription: func(dtProps map[string]any, m map[string]stringAndAny) (_ map[string]any, err error) {
		for k, v := range m {
			if v.str != "" {
				v.val, err = strconv.ParseFloat(v.str, 64)
			}
			m[k] = v
		}
		return map[string]any{}, nil
	},
	toGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
		val, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		if !noChecks {
			if dt.MinInclusive != nil && val < dt.MinInclusive.(float64) {
				return nil, errors.New("value smaller than minimum")
			}
			if dt.MaxInclusive != nil && val > dt.MaxInclusive.(float64) {
				return nil, errors.New("value greater than maximum")
			}
			if dt.MinExclusive != nil && val <= dt.MinExclusive.(float64) {
				return nil, errors.New("value smaller than exclusive minimum")
			}
			if dt.MaxExclusive != nil && val >= dt.MaxExclusive.(float64) {
				return nil, errors.New("value greater than exclusive maximum")
			}
		}
		return val, nil
	},
	toString: func(dt *Datatype, x any) (string, error) {
		return fmt.Sprintf("%g", x.(float64)), nil
	},
	sqlType: "REAL",
	toSql: func(dt *Datatype, x any) (any, error) {
		return x.(float64), nil
	},
}
