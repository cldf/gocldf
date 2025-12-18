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
	return *New(0, result)
}

func TestColumn_CanonicalName(t *testing.T) {
	col := makeCol(`{"name":"The Name"}`)
	want := "The Name"
	if want != col.CanonicalName() {
		t.Errorf(`problem`)
	}
	col = makeCol(`{"name":"The Name", "propertyUrl": "http://cldf.clld.org/#prop"}`)
	want = "cldf_prop"
	if want != col.CanonicalName() {
		t.Errorf(`problem: %q vs %q`, want, col.CanonicalName())
	}
	col = makeCol(`{}`)
	want = "Col_1"
	if want != col.CanonicalName() {
		t.Errorf(`problem: %q vs %q`, want, col.CanonicalName())
	}
}

func TestColumn_Datatype(t *testing.T) {
	col := makeCol(`{"name": "The Name"}`)
	want := "string"
	if want != col.Datatype.Base {
		t.Errorf(`problem`)
	}
	col = makeCol(`{"datatype": "boolean"}`)
	want = "boolean"
	if want != col.Datatype.Base {
		t.Errorf(`problem: %q vs %q`, want, col.CanonicalName())
	}
	col = makeCol(`{"datatype": {"base": "boolean"}}`)
	want = "boolean"
	if want != col.Datatype.Base {
		t.Errorf(`problem: %q vs %q`, want, col.CanonicalName())
	}
}
