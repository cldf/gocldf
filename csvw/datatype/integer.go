package datatype

import (
	"errors"
	"strconv"
)

var Integer = baseType{
	getDerivedDescription: func(dtProps map[string]any) (map[string]any, error) {
		return map[string]any{}, nil
	},
	setValueConstraints: func(m map[string]stringAndAny) (err error) {
		for k, v := range m {
			if v.str != "" {
				v.val, err = strconv.Atoi(v.str)
			}
			m[k] = v
		}
		return
	},
	toGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
		val, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		if !noChecks {
			if dt.MinInclusive != nil && val < dt.MinInclusive.(int) {
				return nil, errors.New("value smaller than minimum")
			}
			if dt.MaxInclusive != nil && val > dt.MaxInclusive.(int) {
				return nil, errors.New("value greater than maximum")
			}
			if dt.MinExclusive != nil && val <= dt.MinExclusive.(int) {
				return nil, errors.New("value smaller than exclusive minimum")
			}
			if dt.MaxExclusive != nil && val >= dt.MaxExclusive.(int) {
				return nil, errors.New("value greater than exclusive maximum")
			}
		}
		return val, nil
	},
	toString: func(dt *Datatype, x any) (string, error) {
		return strconv.Itoa(x.(int)), nil
	},
	sqlType: "INTEGER",
	toSql: func(dt *Datatype, x any) (any, error) {
		return x.(int), nil
	},
}
