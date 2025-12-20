package datatype

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type Datatype struct {
	Base               string
	Length             int
	MinLength          int
	MaxLength          int
	Minimum            float64
	Maximum            float64
	MinInclusive       float64
	MaxInclusive       float64
	MinExclusive       float64
	MaxExclusive       float64
	DerivedDescription map[string]any
}

/*
Datatypes in CSVW can specify extra behaviour, e.g. to guide parsing of values.
This is behaviour is specfied via additional items in the datatype JSON object.

The CSVW spec defines the following steps to parse a string value in a csv cell:
 1. unless the datatype base is string, json, xml, html or anyAtomicType,
    replace all carriage return (#xD), line feed (#xA), and tab (#x9) characters with space characters.
 2. unless the datatype base is string, json, xml, html, anyAtomicType, or normalizedString,
    strip leading and trailing whitespace from the string value and replace all instances of
    two or more whitespace characters with a single space character.
 3. if the normalized string is an empty string, apply the remaining steps to the string given by the column default annotation.
 4. if the column separator annotation is not null and the normalized string is an empty string, the cell value is an empty list. If the column required annotation is true, add an error to the list of errors for the cell.
 5. if the column separator annotation is not null, the cell value is a list of values; set the list annotation on the cell to true, and create the cell value created by:
    5.1 if the normalized string is the same as any one of the values of the column null annotation, then the resulting value is null.
    5.2 split the normalized string at the character specified by the column separator annotation.
    5.3 unless the datatype base is string or anyAtomicType, strip leading and trailing whitespace from these strings.
    5.4 applying the remaining steps to each of the strings in turn.
 6. if the string is an empty string, apply the remaining steps to the string given by the column default annotation.
 7. if the string is the same as any one of the values of the column null annotation, then the resulting value is null. If the column separator annotation is null and the column required annotation is true, add an error to the list of errors for the cell.
 8. parse the string using the datatype format if one is specified, as described below to give a value with an associated datatype. If the datatype base is string, or there is no datatype, the value has an associated language from the column lang annotation. If there are any errors, add them to the list of errors for the cell; in this case the value has a datatype of string; if the datatype base is string, or there is no datatype, the value has an associated language from the column lang annotation.
 9. validate the value based on the length constraints described in section 4.6.1 Length Constraints, the value constraints described in section 4.6.2 Value Constraints and the datatype format annotation if one is specified, as described below. If there are any errors, add them to the list of errors for the cell.
*/

type BaseType struct {
	// GetDerivedDescription is called when instantiating a Datatype object.
	// The result is stored as DerivedDescription member of the Datatype and can be
	// accessed from ToGo and ToString via the Datatype passed as first argument.
	GetDerivedDescription func(map[string]any) map[string]any
	ToGo                  func(*Datatype, string) any
	ToString              func(*Datatype, any) string
	SqlType               string
	ToSql                 func(*Datatype, any) any
}

var (
	Boolean = BaseType{
		GetDerivedDescription: func(dtProps map[string]any) map[string]any {
			val, ok := dtProps["format"]
			if ok {
				yesno := strings.Split(val.(string), "|")
				return map[string]any{"true": []string{yesno[0]}, "false": []string{yesno[1]}}
			}
			return map[string]any{"true": []string{"true", "1"}, "false": []string{"false", "0"}}
		},
		ToGo: func(dt *Datatype, s string) any {
			if slices.Contains(dt.DerivedDescription["true"].([]string), s) {
				return true
			} else if slices.Contains(dt.DerivedDescription["false"].([]string), s) {
				return false
			}
			panic("Invalid value for datatype")
		},
		ToString: func(dt *Datatype, x any) string {
			if x.(bool) {
				return dt.DerivedDescription["true"].([]string)[0]
			}
			return dt.DerivedDescription["false"].([]string)[0]
		},
		SqlType: "INTEGER",
		ToSql: func(dt *Datatype, x any) any {
			if x == nil {
				return nil
			}
			if x.(bool) {
				return 1
			}
			return 0
		},
	}
	String = BaseType{
		GetDerivedDescription: func(dtProps map[string]any) map[string]any {
			val, ok := dtProps["format"]
			if ok {
				// FIXME: must make sure regex is anchored on both sides! I.e. wrap in "^$" if necessary.
				return map[string]any{"regex": regexp.MustCompile(val.(string))}
			}
			return map[string]any{"regex": nil}
		},
		ToGo: func(dt *Datatype, s string) any {
			if dt.DerivedDescription["regex"] != nil {
				if !dt.DerivedDescription["regex"].(*regexp.Regexp).MatchString(s) {
					panic(fmt.Sprintf("invalid value: %v", s))
				}
			}
			return s
		},
		ToString: func(dt *Datatype, x any) string {
			return x.(string)
		},
		SqlType: "TEXT",
		ToSql: func(dt *Datatype, x any) any {
			if x == nil {
				return nil
			}
			return x.(string)
		},
	}
	AnyURI = BaseType{
		GetDerivedDescription: func(dtProps map[string]any) map[string]any {
			return map[string]any{}
		},
		ToGo: func(dt *Datatype, s string) any {
			return s
		},
		ToString: func(dt *Datatype, x any) string {
			return x.(string)
		},
		SqlType: "TEXT",
		ToSql: func(dt *Datatype, x any) any {
			if x == nil {
				return nil
			}
			return x.(string)
		},
	}
	Integer = BaseType{
		GetDerivedDescription: func(dtProps map[string]any) map[string]any {
			return map[string]any{}
		},
		ToGo: func(dt *Datatype, s string) any {
			val, err := strconv.Atoi(s)
			if err == nil {
				return val
			}
			panic("Invalid value for integer")
		},
		ToString: func(dt *Datatype, x any) string {
			return x.(string)
		},
		SqlType: "INTEGER",
		ToSql: func(dt *Datatype, x any) any {
			if x == nil {
				return nil
			}
			return x.(int)
		},
	}
	Decimal = BaseType{
		GetDerivedDescription: func(dtProps map[string]any) map[string]any {
			return map[string]any{}
		},
		ToGo: func(dt *Datatype, s string) any {
			val, err := strconv.ParseFloat(s, 64)
			if err == nil {
				return val
			}
			panic("Invalid value for integer")
		},
		ToString: func(dt *Datatype, x any) string {
			return x.(string)
		},
		SqlType: "REAL",
		ToSql: func(dt *Datatype, x any) any {
			if x == nil {
				return nil
			}
			return x.(float64)
		},
	}
	Json = BaseType{
		GetDerivedDescription: func(dtProps map[string]any) map[string]any {
			return map[string]any{}
		},
		ToGo: func(dt *Datatype, s string) any {
			var result any
			err := json.Unmarshal([]byte(s), &result)
			if err != nil {
				panic(fmt.Sprintf("%v: '%v'", err, s))
			}
			return result
		},
		ToString: func(dt *Datatype, x any) string {
			res, err := json.Marshal(x)
			if err != nil {
				panic(err)
			}
			return string(res)
		},
		SqlType: "TEXT",
		ToSql: func(dt *Datatype, x any) any {
			if x == nil {
				return nil
			}
			res, err := json.Marshal(x)
			if err != nil {
				panic(err)
			}
			return string(res)
		},
	}
)
var BaseTypes = map[string]BaseType{
	"boolean": Boolean,
	"string":  String,
	"anyURI":  AnyURI,
	"integer": Integer,
	"decimal": Decimal,
	"json":    Json,
}

func New(jsonCol map[string]interface{}) *Datatype {
	base := "string"
	dtProps := map[string]any{}

	var val, ok = jsonCol["datatype"]
	if ok {
		s, ok := val.(string)
		if ok {
			base = s
		} else {
			base = val.(map[string]interface{})["base"].(string)
			dtProps = val.(map[string]any)
		}
	}
	//fmt.Println(base)
	return &Datatype{
		Base:               base,
		DerivedDescription: BaseTypes[base].GetDerivedDescription(dtProps)}
}

func (dt *Datatype) ToString(val any) string {
	return BaseTypes[dt.Base].ToString(dt, val)
}

func (dt *Datatype) ToGo(s string) any {
	return BaseTypes[dt.Base].ToGo(dt, s)
}

func (dt *Datatype) SqlType() string {
	return BaseTypes[dt.Base].SqlType
}

func (dt *Datatype) ToSql(val any) any {
	return BaseTypes[dt.Base].ToSql(dt, val)
}
