/*
Package datatype provides functionality to read and write atomic cell data according to CSVW.

Reading and writing of cell data in CSVW is governed by a datatype description, consisting of
a base type and additional constraints.

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
package datatype

import (
	"gocldf/internal/jsonutil"
	"strconv"
)

/*
BaseType ties together functions related to converting between string and Go representations of CSVW datatypes.

GetDerivedDescription is called when instantiating a Datatype object.
The result is stored as DerivedDescription member of the Datatype and can be
accessed from ToGo and ToString via the Datatype passed as first argument.

ToGo implements parsing of a string into an appropriately typed Go object.

ToString implements the serialization of the Go object to a string - ideally in
roundtrip-safe way.

SqlType specifies the best matching SQLite data type.

ToSql implements the conversion of the Go object to a suitable object for insertion
into a SQLite database.
*/
type BaseType struct {
	GetDerivedDescription func(map[string]any) (map[string]any, error)
	ToGo                  func(*Datatype, string, bool) (any, error)
	ToString              func(*Datatype, any) (string, error)
	SqlType               string
	ToSql                 func(*Datatype, any) (any, error)
}

// BaseTypes provides a mapping of CSVW data type base names to BaseType instances.
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

// Datatype holds the data related to a CSVW datatype description.
//
//	Base is the name of the data type base
//	Length, etc. are optional length constraint
type Datatype struct {
	Base               string
	Length             int
	MinLength          int
	MaxLength          int
	MinInclusive       any
	MaxInclusive       any
	MinExclusive       any
	MaxExclusive       any
	DerivedDescription map[string]any
}

// New is a factory function to create a Datatype as specified in a JSON description.
func New(jsonCol map[string]interface{}) (*Datatype, error) {
	var (
		s   string
		err error
		//
		minInclusiveS string
		maxInclusiveS string
		minExclusiveS string
		maxExclusiveS string
		minInclusive  any = nil
		maxInclusive  any = nil
		minExclusive  any = nil
		maxExclusive  any = nil
		// We seed the three length constraints with a sentinel value.
		length    = -1
		minLength = -1
		maxLength = -1
		base      = "string"
	)
	dtProps := map[string]any{}

	val, ok := jsonCol["datatype"]
	if ok {
		s, ok = val.(string)
		if ok { // Spec of the form "datatype": "name-of-base"
			base = s
		} else { // Spec is a JSON object "datatype": {...}
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
			// For constraints which must match the base type we first get the string representation.
			minInclusiveS, err = jsonutil.GetString(dtProps, "minimum", "")
			if err != nil {
				return nil, err
			}
			maxInclusiveS, err = jsonutil.GetString(dtProps, "maximum", "")
			if err != nil {
				return nil, err
			}
			if minInclusiveS == "" {
				minInclusiveS, err = jsonutil.GetString(dtProps, "minInclusive", "")
				if err != nil {
					return nil, err
				}
			}
			minExclusiveS, err = jsonutil.GetString(dtProps, "minExclusive", "")
			if err != nil {
				return nil, err
			}
			if maxInclusiveS == "" {
				maxInclusiveS, err = jsonutil.GetString(dtProps, "maxInclusive", "")
				if err != nil {
					return nil, err
				}
			}
			maxExclusiveS, err = jsonutil.GetString(dtProps, "maxExclusive", "")
			if err != nil {
				return nil, err
			}
		}
	}
	// Now convert the constraints appropriately:
	switch base {
	case "integer":
		if minInclusiveS != "" {
			minInclusive, err = strconv.Atoi(minInclusiveS)
		}
		if minExclusiveS != "" {
			minExclusive, err = strconv.Atoi(minExclusiveS)
		}
		if maxInclusiveS != "" {
			maxInclusive, err = strconv.Atoi(maxInclusiveS)
		}
		if maxExclusiveS != "" {
			maxExclusive, err = strconv.Atoi(maxExclusiveS)
		}
	case "decimal", "float", "number", "double":
		if minInclusiveS != "" {
			minInclusive, err = strconv.ParseFloat(minInclusiveS, 64)
		}
		if minExclusiveS != "" {
			minExclusive, err = strconv.ParseFloat(minExclusiveS, 64)
		}
		if maxInclusiveS != "" {
			maxInclusive, err = strconv.ParseFloat(maxInclusiveS, 64)
		}
		if maxExclusiveS != "" {
			maxExclusive, err = strconv.ParseFloat(maxExclusiveS, 64)
		}
	}
	// We compute the derived description map once at instantiation.
	dd, err := BaseTypes[base].GetDerivedDescription(dtProps)
	if err != nil {
		return nil, err
	}
	// FIXME: validate constraints!
	res := &Datatype{
		Base:               base,
		DerivedDescription: dd,
		Length:             length,
		MinLength:          minLength,
		MaxLength:          maxLength,
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

func (dt *Datatype) ToGo(s string, noChecks bool) (any, error) {
	return BaseTypes[dt.Base].ToGo(dt, s, noChecks)
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
