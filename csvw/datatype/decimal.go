package datatype

import (
	"errors"
	"fmt"
	"strconv"
)

func getOptionalFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	num, _ := strconv.ParseFloat(s, 64)
	return num, true
}

var Decimal = BaseType{
	GetDerivedDescription: func(dtProps map[string]any) (map[string]any, error) {
		return map[string]any{}, nil
	},
	ToGo: func(dt *Datatype, s string, checkConstraints bool) (any, error) {
		val, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		if checkConstraints {
			num, ok := dt.Minimum.(float64)
			if ok && val < num {
				return nil, errors.New("value smaller than minimum")
			}
			num, ok = dt.Maximum.(float64)
			if ok && val > num {
				return nil, errors.New("value greater than maximum")
			}
			num, ok = getOptionalFloat(dt.MinInclusive)
			if ok && val < num {
				return nil, errors.New("value smaller than minimum")
			}
			num, ok = getOptionalFloat(dt.MaxInclusive)
			if ok && val > num {
				return nil, errors.New("value greater than maximum")
			}
			num, ok = getOptionalFloat(dt.MinExclusive)
			if ok && val <= num {
				return nil, errors.New("value smaller than exclusive minimum")
			}
			num, ok = getOptionalFloat(dt.MaxExclusive)
			if ok && val >= num {
				return nil, errors.New("value greater than exclusive maximum")
			}
		}
		return val, nil
	},
	ToString: func(dt *Datatype, x any) (string, error) {
		return fmt.Sprintf("%g", x.(float64)), nil
	},
	SqlType: "REAL",
	ToSql: func(dt *Datatype, x any) (any, error) {
		return x.(float64), nil
	},
}
