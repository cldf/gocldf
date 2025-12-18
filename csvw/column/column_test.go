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
}
