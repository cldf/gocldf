package jsonutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
)

func ReadObject(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func GetString(jsonObject map[string]any, attr string, defaultValue string) (string, error) {
	var res string
	val, ok := jsonObject[attr]
	if ok {
		if valN, ok := val.(float64); ok {
			return strconv.FormatFloat(valN, 'g', -1, 64), nil
		}
		res, ok = val.(string)
		if ok {
			return res, nil
		}
		return res, errors.New(attr + " is not a string")
	}
	return defaultValue, nil
}

func GetRune(jsonObject map[string]any, attr string, defaultValue rune) (rune, error) {
	val, ok := jsonObject[attr]
	if ok {
		if val == nil {
			return rune(0), nil
		}
		if s, ok := val.(string); ok {
			runes := []rune(s)
			if len(runes) == 1 {
				return runes[0], nil
			}
			return rune(0), errors.New(fmt.Sprintf("%v must be one character", attr))
		}
		return rune(0), errors.New(fmt.Sprintf("invalid %v", attr))
	}
	return defaultValue, nil
}

func GetInt(jsonObject map[string]any, attr string, defaultValue int) (int, error) {
	val, ok := jsonObject[attr]
	if ok {
		if b, ok := val.(float64); ok {
			return int(b), nil
		}
		return defaultValue, errors.New(fmt.Sprintf("invalid %v", attr))
	}
	return defaultValue, nil
}

func GetBool(jsonObject map[string]any, attr string, defaultValue bool) (bool, error) {
	val, ok := jsonObject[attr]
	if ok {
		if b, ok := val.(bool); ok {
			return b, nil
		}
		return defaultValue, errors.New(fmt.Sprintf("invalid %v", attr))
	}
	return defaultValue, nil
}

func GetStringArray(jsonObject map[string]any, attr string) ([]string, error) {
	res := make([]string, 0)
	val, ok := jsonObject[attr]
	if ok {
		for _, n := range val.([]interface{}) {
			s, ok := n.(string)
			if ok {
				res = append(res, s)
			} else {
				return res, errors.New(fmt.Sprintf("invalid %v", attr))
			}
		}
	}
	return res, nil
}
