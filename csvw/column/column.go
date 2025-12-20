package column

import (
	"gocldf/csvw/datatype"
	"slices"
	"strconv"
	"strings"
)

type Column struct {
	Name          string
	CanonicalName string
	PropertyUrl   string
	Datatype      datatype.Datatype
	Separator     string
	Null          []string
}

func New(index int, jsonCol map[string]interface{}) *Column {
	var (
		name          string = ""
		purl          string = ""
		sep           string = ""
		canonicalName string = ""
	)
	null := make([]string, 0)
	val, ok := jsonCol["name"]
	if ok {
		name = val.(string)
	} else {
		name = "Col_" + strconv.Itoa(index+1)
	}
	val, ok = jsonCol["separator"]
	if ok {
		sep = val.(string)
	}
	val, ok = jsonCol["propertyUrl"]
	if ok {
		purl = val.(string)
	}
	val, ok = jsonCol["null"]
	if ok {
		for _, n := range val.([]interface{}) {
			s, ok := n.(string)
			if ok {
				null = append(null, s)
			}
		}
	}
	if len(null) == 0 {
		null = append(null, "")
	}
	if purl != "" && strings.HasPrefix(purl, "http://cldf.clld.org") {
		parts := strings.Split(purl, "#")
		canonicalName = "cldf_" + parts[len(parts)-1]
	} else {
		canonicalName = name
	}
	return &Column{
		Name:          name,
		CanonicalName: canonicalName,
		PropertyUrl:   purl,
		Datatype:      *datatype.New(jsonCol),
		Separator:     sep,
		Null:          null}
}

func (column *Column) ToGo(s string, split bool) any {
	if slices.Contains(column.Null, s) {
		if split && column.Separator != "" {
			return make([]string, 0)
		}
		return nil
	}
	if split && column.Separator != "" {
		fields := strings.Split(s, column.Separator)
		res := make([]string, len(fields))
		for i, field := range fields {
			res[i] = column.ToGo(field, false).(string)
		}
		return res
	}
	return column.Datatype.ToGo(s)
}

func (column *Column) ToString(x any) string {
	if x == nil {
		return column.Null[0]
	}
	return column.Datatype.ToString(x)
}
