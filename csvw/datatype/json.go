package datatype

import "encoding/json"

var Json = baseType{
	GetDerivedDescription: func(dtProps map[string]any) (map[string]any, error) {
		return map[string]any{}, nil
	},
	SetValueConstraints: zeroSetValueConstraints,
	ToGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
		var result any
		err := json.Unmarshal([]byte(s), &result)
		if err != nil {
			return nil, err
		}
		return result, nil
	},
	ToString: func(dt *Datatype, x any) (string, error) {
		res, err := json.Marshal(x)
		if err != nil {
			return "", nil
		}
		return string(res), nil
	},
	SqlType: "TEXT",
	ToSql: func(dt *Datatype, x any) (any, error) {
		res, err := json.Marshal(x)
		if err != nil {
			return nil, err
		}
		return string(res), nil
	},
}
