package datatype

import "encoding/json"

func _jsonToString(dt *Datatype, x any) (string, error) {
	res, err := json.Marshal(x)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

var Json = baseType{
	getDerivedDescription: func(dtProps map[string]any, _ map[string]stringAndAny) (map[string]any, error) {
		return map[string]any{}, nil
	},
	toGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
		var result any
		err := json.Unmarshal([]byte(s), &result)
		if err != nil {
			return nil, err
		}
		return result, nil
	},
	toString: _jsonToString,
	sqlType:  "TEXT",
	toSql: func(dt *Datatype, x any) (any, error) {
		return _jsonToString(dt, x)
	},
}
