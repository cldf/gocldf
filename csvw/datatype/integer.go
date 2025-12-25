package datatype

import (
	"errors"
	"strconv"
)

func getOptionalInt(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	num, _ := strconv.Atoi(s)
	return num, true
}

var Integer = BaseType{
	GetDerivedDescription: func(dtProps map[string]any) (map[string]any, error) {
		return map[string]any{}, nil
	},
	ToGo: func(dt *Datatype, s string, checkConstraints bool) (any, error) {
		val, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		if checkConstraints {
			num, ok := dt.Minimum.(int)
			if ok && val < num {
				return nil, errors.New("value smaller than minimum")
			}
			num, ok = dt.Maximum.(int)
			if ok && val > num {
				return nil, errors.New("value greater than maximum")
			}
			num, ok = getOptionalInt(dt.MinInclusive)
			if ok && val < num {
				return nil, errors.New("value smaller than minimum")
			}
			num, ok = getOptionalInt(dt.MaxInclusive)
			if ok && val > num {
				return nil, errors.New("value greater than maximum")
			}
			num, ok = getOptionalInt(dt.MinExclusive)
			if ok && val <= num {
				return nil, errors.New("value smaller than exclusive minimum")
			}
			num, ok = getOptionalInt(dt.MaxExclusive)
			if ok && val >= num {
				return nil, errors.New("value greater than exclusive maximum")
			}
		}
		return val, nil
	},
	ToString: func(dt *Datatype, x any) (string, error) {
		return strconv.Itoa(x.(int)), nil
	},
	SqlType: "INTEGER",
	ToSql: func(dt *Datatype, x any) (any, error) {
		return x.(int), nil
	},
}
