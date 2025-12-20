package datatype

import (
	"encoding/json"
	"fmt"
	"testing"
)

func makeDatatype(jsonString string) Datatype {
	var result map[string]interface{}

	err := json.Unmarshal([]byte(fmt.Sprintf(`{"datatype":%v}`, jsonString)), &result)
	if err != nil {
		panic(err)
	}
	return *New(result)
}

func TestDatatype_String(t *testing.T) {
	dt := makeDatatype(`{"base":"string"}`)
	want := "mystring"
	if val, err := dt.ToGo("mystring"); err != nil {
		if val != want {
			t.Errorf(`problem`)
		}
	}
	dt = makeDatatype(`{"base":"string","format":"^[s]+$"}`)
	want = "sss"
	if val, err := dt.ToGo("sss"); err != nil {
		if val != want {
			t.Errorf(`problem`)
		}
	}
	if _, err := dt.ToGo("sst"); err == nil {
		t.Errorf("problem")
	}
}

func TestDatatype_Boolean(t *testing.T) {
	dt := makeDatatype(`{"base":"boolean"}`)
	if val, err := dt.ToGo("false"); err != nil {
		if val != false {
			t.Errorf(`problem`)
		}
	}
	dt = makeDatatype(`{"base":"boolean","format":"yes|no"}`)
	if val, err := dt.ToGo("no"); err != nil {
		if val != false {
			t.Errorf(`problem`)
		}
	}
}

func TestColumn_RoundtripValue(t *testing.T) {
	var tests = []struct {
		datatype string
		input    string
	}{
		{`{"base": "boolean","format":"yes|no"}`, "yes"},
		{`{"base": "boolean","format":"yes|no"}`, "no"},
		{`{"base": "integer"}`, "5"},
		{`{"base":"json"}`, `{"k":5}`},
		{`{"base":"string"}`, "äöü"},
	}
	for _, tt := range tests {
		t.Run("Roundtrip", func(t *testing.T) {
			dt := makeDatatype(tt.datatype)
			val, err := dt.ToGo(tt.input)
			if err != nil {
				if val, err := dt.ToString(val); err != nil {
					if val != tt.input {
						t.Errorf(`problem: %v vs %v`, tt.input, dt)
					}
				}
			}
		})
	}
}
