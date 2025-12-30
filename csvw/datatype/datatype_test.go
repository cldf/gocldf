package datatype

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
)

func makeDatatype(jsonString string) Datatype {
	var result map[string]interface{}

	err := json.Unmarshal([]byte(fmt.Sprintf(`{"datatype":%v}`, jsonString)), &result)
	if err != nil {
		panic(err)
	}
	dt, err := New(result)
	if err != nil {
		panic(err)
	}
	return *dt
}

func TestDatatype_String(t *testing.T) {
	dt := makeDatatype(`{"base":"string"}`)
	want := "mystring"
	if val, err := dt.ToGo("mystring", false); err != nil {
		if val != want {
			t.Errorf(`problem`)
		}
	}
	dt = makeDatatype(`{"base":"string","format":"^[s]+$"}`)
	want = "sss"
	if val, err := dt.ToGo("sss", false); err != nil {
		if val != want {
			t.Errorf(`problem`)
		}
	}
	if _, err := dt.ToGo("sst", false); err == nil {
		t.Errorf("problem")
	}
}

func TestDatatype_Boolean(t *testing.T) {
	dt := makeDatatype(`{"base":"boolean"}`)
	if val, err := dt.ToGo("false", true); err != nil {
		if val != false {
			t.Errorf(`problem`)
		}
	}
	dt = makeDatatype(`{"base":"boolean","format":"yes|no"}`)
	if val, err := dt.ToGo("no", false); err != nil {
		if val != false {
			t.Errorf(`problem`)
		}
	}
}

func TestDatatype_ToGo(t *testing.T) {
	var tests = []struct {
		datatype  string
		input     string
		assertion func(any) bool
	}{
		{
			`"boolean"`,
			"1",
			func(x any) bool { return x.(bool) == true }},
		{
			`"integer"`,
			"5",
			func(x any) bool { return x.(int) == 5 }},
		{
			`"float"`,
			"5.0",
			func(x any) bool { return x.(float64) == 5.0 }},
		{
			`"decimal"`,
			"1.1",
			func(x any) bool { return x.(float64) == 1.1 }},
		{
			`"anyURI"`,
			"http://example.org",
			func(x any) bool { return x.(*url.URL).Host == "example.org" }},
	}
	for _, tt := range tests {
		t.Run("toGo", func(t *testing.T) {
			dt := makeDatatype(tt.datatype)
			val, err := dt.ToGo(tt.input, true)
			if err == nil {
				if !tt.assertion(val) {
					t.Errorf(`problem: %v vs %v`, tt.input, val)
				}
			}
		})
	}
}

func TestDatatype_ToGoError(t *testing.T) {
	var tests = []struct {
		datatype string
		input    string
	}{
		{`"boolean"`, "x"},
		{`"integer"`, "x"},
		{`"decimal"`, "1.x"},
		{`{"base":"decimal","minimum":-2.2}`, "-2.3"},
		{`{"base":"decimal","minInclusive":-2.2}`, "-2.3"},
		{`{"base":"decimal","minExclusive":"0"}`, "0"},
		{`{"base":"datetime","maximum":"2018-12-10T20:20:20"}`, "2019-12-10T20:20:20"},
		{`{"base":"integer","maxExclusive":"5"}`, "5"},
		{`{"base":"string","length":3}`, "ab"},
		{`{"base":"string","minLength":3}`, "ab"},
		{`{"base":"string","maxLength":3}`, "abcd"},
		{`"anyURI"`, "12:/example.org"},
	}
	for _, tt := range tests {
		t.Run("toGo", func(t *testing.T) {
			dt := makeDatatype(tt.datatype)
			val, err := dt.ToGo(tt.input, false)
			if err == nil {
				t.Errorf(`problem: %v vs %v`, tt.input, val)
			}
		})
	}
}

func TestDatatype_RoundtripValue(t *testing.T) {
	var tests = []struct {
		datatype string
		input    string
	}{
		{`{"base": "boolean","format":"yes|no"}`, "yes"},
		{`{"base": "boolean","format":"yes|no"}`, "no"},
		{`{"base": "integer"}`, "5"},
		{`{"base": "decimal"}`, "1.1"},
		{`{"base":"json"}`, `{"k":5}`},
		{`{"base":"string"}`, "äöü"},
		{`{"base":"anyURI"}`, "http://example.org"},
		{`{"base":"datetime"}`, "2018-12-10T20:20:20"},
		{`{"base":"datetime","format":"yyyy-MM-dd HH:mm X"}`, "2018-12-10 20:20 +0530"},
		{`{"base":"datetime","format":"yyyy-MM-ddTHH:mm"}`, "2018-12-10T20:20"},
	}
	for _, tt := range tests {
		t.Run("Roundtrip", func(t *testing.T) {
			dt := makeDatatype(tt.datatype)
			val, err := dt.ToGo(tt.input, true)
			if err == nil {
				if val, err := dt.ToString(val); err == nil {
					if val != tt.input {
						t.Errorf(`problem: %v vs %v`, tt.input, val)
					}
				}
			} else {
				t.Error(err)
			}
		})
	}
}
