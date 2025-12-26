package jsonutil

import (
	"errors"
	"fmt"
	"strconv"
)

func GetString(jsonObject map[string]any, attr string, defaultValue string) (string, error) {
	val, ok := jsonObject[attr]
	if ok {
		if valN, ok := val.(float64); ok {
			return strconv.FormatFloat(valN, 'g', -1, 64), nil
		}
		return val.(string), nil
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
