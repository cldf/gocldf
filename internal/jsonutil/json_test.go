package jsonutil

import (
	"encoding/json"
	"testing"
)

func makeJson(s string) map[string]any {
	var jsonObj map[string]any
	json.Unmarshal([]byte(s), &jsonObj)
	return jsonObj
}

func Test_ReadObject(t *testing.T) {
	json, _ := ReadObject("testdata/test.json")
	val, _ := json["test"]
	if val.(string) != "name" {
		t.Errorf(`problem: "%v"`, json)
	}
}

func Test_GetString(t *testing.T) {
	json, _ := GetString(makeJson(`{"id": 123, "name": "test"}`), "name", "x")
	if json != "test" {
		t.Errorf(`problem: "%v" vs "test"`, json)
	}
	json, _ = GetString(makeJson(`{"id": 123, "name": "test"}`), "missing", "x")
	if json != "x" {
		t.Errorf(`problem: "%v" vs "x"`, json)
	}
}

func Test_GetStringArray(t *testing.T) {
	json, _ := GetStringArray(makeJson(`{"name": ["a","b"]}`), "name")
	if json[0] != "a" {
		t.Errorf(`problem: "%v" vs ["a"]`, json)
	}
}
