package datatype

import (
	"encoding/json"
	"testing"
)

func makeDatatype(jsonString string) Datatype {
	var result map[string]interface{}

	err := json.Unmarshal([]byte(jsonString), &result)
	if err != nil {
		panic(err)
	}
	return *New(result)
}

func TestDatatype_String(t *testing.T) {
	dt := makeDatatype(`{"datatype":{"base":"string"}}`)
	want := "mystring"
	if want != dt.ToGo("mystring") {
		t.Errorf(`problem`)
	}
	dt = makeDatatype(`{"datatype":{"base":"string","format":"^[s]+$"}}`)
	want = "sss"
	if want != dt.ToGo("sss") {
		t.Errorf(`problem`)
	}
	defer func() {
		if r := recover(); r != "invalid value" {
			t.Errorf("Unexpected panic: %v", r)
		}
	}()
	func() {
		dt.ToGo("sst")
	}()
}

func TestDatatype_Boolean(t *testing.T) {
	dt := makeDatatype(`{"datatype":{"base":"boolean"}}`)
	want := false
	if want != dt.ToGo("false") {
		t.Errorf(`problem`)
	}
	dt = makeDatatype(`{"datatype":{"base":"boolean","format":"yes|no"}}`)
	want = false
	if want != dt.ToGo("no") {
		t.Errorf(`problem`)
	}
}
