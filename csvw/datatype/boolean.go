package datatype

import (
	"errors"
	"slices"
	"strings"
)

var Boolean = baseType{
	GetDerivedDescription: func(dtProps map[string]any) (map[string]any, error) {
		val, ok := dtProps["format"]
		if ok {
			yesno := strings.Split(val.(string), "|")
			return map[string]any{"true": []string{yesno[0]}, "false": []string{yesno[1]}}, nil
		}
		return map[string]any{"true": []string{"true", "1"}, "false": []string{"false", "0"}}, nil
	},
	SetValueConstraints: zeroSetValueConstraints,
	ToGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
		if slices.Contains(dt.DerivedDescription["true"].([]string), s) {
			return true, nil
		}
		if slices.Contains(dt.DerivedDescription["false"].([]string), s) {
			return false, nil
		}
		return nil, errors.New("invalid value")
	},
	ToString: func(dt *Datatype, x any) (string, error) {
		if x.(bool) {
			return dt.DerivedDescription["true"].([]string)[0], nil
		}
		return dt.DerivedDescription["false"].([]string)[0], nil
	},
	SqlType: "INTEGER",
	ToSql: func(dt *Datatype, x any) (any, error) {
		if x.(bool) {
			return 1, nil
		}
		return 0, nil
	},
}
