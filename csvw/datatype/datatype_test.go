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

func TestDatatype_NewError(t *testing.T) {
	var tests = []struct {
		jsonString string
	}{
		{`{"format":"(a|"}`}, // incomplete regex
		{`{"format":2}`},
		{`{"length":"x"}`},
		{`{"minLength":false}`},
		{`{"maxLength":null}`},
		{`{"base":"number","minimum":null}`},
		{`{"base":"number","maximum":null}`},
		{`{"base":"number","maxExclusive":null}`},
		{`{"base":"dateTimeStamp","format":"HH:mm"}`},
		{`{"base":"dateTimeStamp","format":null}`},
		{`{"base":"dateTimeStamp","format":"xyz"}`},
	}
	for _, tt := range tests {
		t.Run("toGo", func(t *testing.T) {
			var result map[string]interface{}

			err := json.Unmarshal([]byte(fmt.Sprintf(`{"datatype":%v}`, tt.jsonString)), &result)
			if err != nil {
				panic(err)
			}
			_, err = New(result)
			if err == nil {
				t.Errorf(`problem: %v`, tt.jsonString)
			}
		})
	}

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
	if v, err := dt.ToSql(nil); v != nil || err != nil {
		t.Errorf("problem")
	}
	if v, err := dt.ToSql("word"); v != "word" || err != nil {
		t.Errorf("problem")
	}
	if dt.SqlType() != "TEXT" {
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

func TestDatatype_ToStringError(t *testing.T) {
	var tests = []struct {
		datatype string
		input    any
	}{
		{`"json"`, func() {}},
	}
	for _, tt := range tests {
		t.Run("toString error", func(t *testing.T) {
			dt := makeDatatype(tt.datatype)
			val, err := dt.ToString(tt.input)
			if err == nil {
				t.Errorf(`problem: %v vs %v`, tt.input, val)
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
		{`{"base":"date","format":"yyyy-MM-ddX"}`, "2018-12-10 22:33:11Z"},
		{`{"base":"date","minimum":"2000-01-01"}`, "1999-12-31"},
		{`{"base":"date","maximum":"2000-01-01"}`, "2999-12-31"},
		{`{"base":"date","minExclusive":"2000-01-01"}`, "2000-01-01"},
		{`{"base":"date","maxExclusive":"2000-01-01"}`, "2000-01-01"},
		{`"json"`, `[1,`},
		{`"binary"`, `space is not allowed in base64`},
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
		{`{"base": "binary"}`, "SGVsbG8gV29ybGQ="},
		{`{"base": "integer"}`, "5"},
		{`{"base": "decimal"}`, "1.1"},
		{`{"base":"json"}`, `{"k":5}`},
		{`{"base":"string"}`, "äöü"},
		{`{"base":"anyURI"}`, "http://example.org"},
		{`{"base":"time"}`, "11:12:13"},
		{`{"base":"date","format":"yyyy-MM-ddX"}`, "2018-12-10Z"},
		{`{"base":"datetime"}`, "2018-12-10T20:20:20"},
		{`{"base":"dateTimeStamp"}`, "2018-12-10T20:20:20Z"},
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

func TestDatatype_ToSql(t *testing.T) {
	var tests = []struct {
		datatype string
		value    any
		sql      any
	}{
		{`"boolean"`, true, 1},
		{`"boolean"`, false, 0},
		{`"anyURI"`, &url.URL{Scheme: "https", Host: "example.org"}, "https://example.org"},
		{`"decimal"`, 2.2, 2.2},
		{`"json"`, []string{"a", "b"}, `["a","b"]`},
	}
	for _, tt := range tests {
		t.Run("ToSql", func(t *testing.T) {
			dt := makeDatatype(tt.datatype)
			val, err := dt.ToSql(tt.value)
			if err == nil {
				if val != tt.sql {
					t.Errorf(`problem: %v vs %v`, tt.value, val)
				}
			} else {
				t.Error(err)
			}
		})
	}
}
