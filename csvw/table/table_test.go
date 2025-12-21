package table

/*
func makeTable(url string, schema string) Table {
	var result map[string]interface{}

	err := json.Unmarshal([]byte(jsonString), &result)
	if err != nil {
		panic(err)
	}
	tbl, err := New(result)
	if err != nil {
		panic(err)
	}
	return *tbl
}

func TestTable_t(t *testing.T) {
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
*/
