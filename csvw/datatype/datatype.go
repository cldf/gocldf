package datatype

import (
	"gocldf/internal/jsonutil"
	"strconv"
)

type BaseType struct {
	// GetDerivedDescription is called when instantiating a Datatype object.
	// The result is stored as DerivedDescription member of the Datatype and can be
	// accessed from ToGo and ToString via the Datatype passed as first argument.
	GetDerivedDescription func(map[string]any) (map[string]any, error)
	ToGo                  func(*Datatype, string, bool) (any, error)
	ToString              func(*Datatype, any) (string, error)
	SqlType               string
	ToSql                 func(*Datatype, any) (any, error)
}

var BaseTypes = map[string]BaseType{
	"boolean": Boolean,
	"string":  String,
	"anyURI":  AnyURI,
	"integer": Integer,
	"decimal": Decimal,
	"float":   Decimal,
	"number":  Decimal,
	"double":  Decimal,
	"json":    Json,
}

type Datatype struct {
	Base               string
	Length             int
	MinLength          int
	MaxLength          int
	Minimum            any
	Maximum            any
	MinInclusive       string
	MaxInclusive       string
	MinExclusive       string
	MaxExclusive       string
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

func New(jsonCol map[string]interface{}) (*Datatype, error) {
	var (
		err error
		//
		minimumStr   string
		maximumStr   string
		minimum      any
		maximum      any
		minInclusive string
		maxInclusive string
		minExclusive string
		maxExclusive string
	)
	// We seed the three length constraints with a sentinel value.
	length := -1
	minLength := -1
	maxLength := -1

	base := "string"
	dtProps := map[string]any{}

	var val, ok = jsonCol["datatype"]
	if ok {
		s, ok := val.(string)
		if ok {
			base = s
		} else {
			// val is JSON object
			dtProps = val.(map[string]any)
			base = dtProps["base"].(string)
			length, err = jsonutil.GetInt(dtProps, "length", -1)
			if err != nil {
				return nil, err
			}
			minLength, err = jsonutil.GetInt(dtProps, "minLength", -1)
			if err != nil {
				return nil, err
			}
			maxLength, err = jsonutil.GetInt(dtProps, "maxLength", -1)
			if err != nil {
				return nil, err
			}
			minimumStr, err = jsonutil.GetString(dtProps, "minimum", "")
			if err != nil {
				return nil, err
			}
			maximumStr, err = jsonutil.GetString(dtProps, "maximum", "")
			if err != nil {
				return nil, err
			}
			minInclusive, err = jsonutil.GetString(dtProps, "minInclusive", "")
			if err != nil {
				return nil, err
			}
			minExclusive, err = jsonutil.GetString(dtProps, "minExclusive", "")
			if err != nil {
				return nil, err
			}
			maxInclusive, err = jsonutil.GetString(dtProps, "maxInclusive", "")
			if err != nil {
				return nil, err
			}
			maxExclusive, err = jsonutil.GetString(dtProps, "maxExclusive", "")
			if err != nil {
				return nil, err
			}
		}
	}
	dd, err := BaseTypes[base].GetDerivedDescription(dtProps)
	if err != nil {
		return nil, err
	}
	switch base {
	case "integer":
		if minimumStr != "" {
			minimum, err = strconv.Atoi(minimumStr)
		}
		if maximumStr != "" {
			maximum, err = strconv.Atoi(maximumStr)
		}
	case "decimal":
		if minimumStr != "" {
			minimum, err = strconv.ParseFloat(minimumStr, 64)
		}
		if maximumStr != "" {
			maximum, err = strconv.ParseFloat(maximumStr, 64)
		}
	}
	res := &Datatype{
		Base:               base,
		DerivedDescription: dd,
		Length:             length,
		MinLength:          minLength,
		MaxLength:          maxLength,
		Minimum:            minimum,
		Maximum:            maximum,
		MinInclusive:       minInclusive,
		MinExclusive:       minExclusive,
		MaxInclusive:       maxInclusive,
		MaxExclusive:       maxExclusive,
	}
	return res, nil
}

func (dt *Datatype) ToString(val any) (string, error) {
	return BaseTypes[dt.Base].ToString(dt, val)
}

func (dt *Datatype) ToGo(s string) (any, error) {
	return BaseTypes[dt.Base].ToGo(dt, s, true)
}

func (dt *Datatype) SqlType() string {
	return BaseTypes[dt.Base].SqlType
}

func (dt *Datatype) ToSql(val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	return BaseTypes[dt.Base].ToSql(dt, val)
}
