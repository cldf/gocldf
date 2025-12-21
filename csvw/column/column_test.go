package column

import (
	"encoding/json"
	"testing"
)

func makeCol(jsonString string) Column {
	var result map[string]interface{}

	err := json.Unmarshal([]byte(jsonString), &result)
	if err != nil {
		panic(err)
	}
	col, err := New(0, result)
	if err != nil {
		panic(err)
	}
	return *col
}

func TestColumn_CanonicalName(t *testing.T) {
	var tests = []struct {
		jsonCol string
		input   string
	}{
		{`{"name":"The Name"}`, "The Name"},
		{`{"name":"The Name", "propertyUrl": "http://cldf.clld.org/#prop"}`, "cldf_prop"},
		{`{}`, "Col_1"},
	}
	for _, tt := range tests {
		t.Run("CanonicalName", func(t *testing.T) {
			col := makeCol(tt.jsonCol)
			if tt.input != col.CanonicalName {
				t.Errorf(`problem: %q vs %q`, tt.input, col.CanonicalName)
			}
		})
	}
}

func TestColumn_Datatype(t *testing.T) {
	var tests = []struct {
		jsonCol string
		input   string
	}{
		{`{}`, "string"},
		{`{"datatype": "boolean"}`, "boolean"},
		{`{"datatype": {"base": "boolean"}}`, "boolean"},
	}
	for _, tt := range tests {
		t.Run("Datatype", func(t *testing.T) {
			col := makeCol(tt.jsonCol)
			if tt.input != col.Datatype.Base {
				t.Errorf(`problem: %q vs %q`, tt.input, col.Datatype.Base)
			}
		})
	}
}
