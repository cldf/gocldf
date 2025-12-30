package datatype

import (
	"encoding/base64"
)

func _base64ToString(dt *Datatype, x any) (string, error) {
	bytes := x.([]byte)
	return base64.StdEncoding.EncodeToString(bytes), nil
}

var base64binary = baseType{
	getDerivedDescription: zeroGetDerivedDescription,
	toGo: func(dt *Datatype, s string, noChecks bool) (any, error) {
		bytes, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, err
		}
		return bytes, nil
	},
	toString: _base64ToString,
	sqlType:  "TEXT",
	toSql: func(dt *Datatype, x any) (any, error) {
		return _base64ToString(dt, x)
	},
}
