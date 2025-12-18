package column

import (
	"strconv"
	"strings"
)

type Datatype struct {
	Base string
}

type Column struct {
	Name        string
	PropertyUrl string
	Datatype    Datatype
	Separator   string
}

func New(index int, jsonCol map[string]interface{}) *Column {
	var name string = ""
	var purl string = ""
	var sep string = ""
	var datatype Datatype = Datatype{}
	val, ok := jsonCol["name"]
	if ok {
		name = val.(string)
	} else {
		name = "Col_" + strconv.Itoa(index)
	}
	val, ok = jsonCol["separator"]
	if ok {
		sep = val.(string)
	}
	val, ok = jsonCol["propertyUrl"]
	if ok {
		purl = val.(string)
	}
	val, ok = jsonCol["datatype"]
	if ok {
		s, ok := val.(string)
		if ok {
			datatype = Datatype{Base: s}
		} else {
			datatype = Datatype{Base: val.(map[string]interface{})["base"].(string)}
		}
	} else {
		datatype = Datatype{Base: "string"}
	}
	return &Column{Name: name, PropertyUrl: purl, Datatype: datatype, Separator: sep}
}

func (column *Column) CanonicalName() string {
	if column.PropertyUrl != "" && strings.HasPrefix(column.PropertyUrl, "http://cldf.clld.org") {
		parts := strings.Split(column.PropertyUrl, "#")
		return "cldf_" + parts[len(parts)-1]
	}
	return column.Name
}

func (column *Column) ToGo(s string, split bool) any {
	if s == "" {
		if split && column.Separator != "" {
			return []interface{}{}
		}
		return nil
	}
	if split && column.Separator != "" {
		fields := strings.Split(s, column.Separator)
		res := make([]interface{}, len(fields))
		for i, field := range fields {
			res[i] = column.ToGo(field, false)
		}
		return res
	}
	if column.Datatype.Base == "boolean" {
		if s == "true" {
			return true
		} else {
			return false
		}
	}
	if column.Datatype.Base == "integer" {
		val, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return val
		}
		panic("invalid integer")
	}
	if column.Datatype.Base == "decimal" {
		val, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return val
		}
		panic("invalid decimal")
	}
	return s
}
