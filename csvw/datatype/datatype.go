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
	"fmt"
	"gocldf/internal/jsonutil"
)

/*
baseType ties together functions related to converting between string and Go representations of CSVW datatypes.

getDerivedDescription is called when instantiating a Datatype object.
The result is stored as DerivedDescription member of the Datatype and can be
accessed from toGo and toString via the Datatype passed as first argument.

setValueConstraints is called when instantiating a Datatype to set the value
constraints that have values which must match the base type.

toGo implements parsing of a string into an appropriately typed Go object.

toString implements the serialization of the Go object to a string - ideally in a
roundtrip-safe way.

sqlType specifies the best matching SQLite data type.

toSql implements the conversion of the Go object to a suitable object for insertion
into a SQLite database.
*/
type baseType struct {
	getDerivedDescription func(map[string]any) (map[string]any, error)
	setValueConstraints   func(map[string]stringAndAny) error
	toGo                  func(*Datatype, string, bool) (any, error)
	toString              func(*Datatype, any) (string, error)
	sqlType               string
	toSql                 func(*Datatype, any) (any, error)
}

func zeroGetDerivedDescription(m map[string]any) (map[string]any, error) {
	if len(m) < 0 {
		return nil, fmt.Errorf("zeroGetDerivedDescription called with %d values", len(m))
	}
	return map[string]any{}, nil
}

func zeroSetValueConstraints(m map[string]stringAndAny) error {
	if len(m) != 4 {
		return fmt.Errorf("zeroGetDerivedDescription called with %d values", len(m))
	}
	return nil
}

// baseTypes provides a mapping of CSVW data type base names to baseType instances.
var baseTypes = map[string]baseType{
	"boolean":  Boolean,
	"string":   String,
	"anyURI":   AnyURI,
	"integer":  Integer,
	"decimal":  Decimal,
	"float":    Decimal,
	"number":   Decimal,
	"double":   Decimal,
	"json":     Json,
	"datetime": Datetime,
	"dateTime": Datetime,
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

type stringAndAny struct {
	str string
	val any
}

// New is a factory function to create a Datatype as specified in a JSON description.
func New(jsonCol map[string]interface{}) (*Datatype, error) {
	var (
		s2a stringAndAny
		s   string
		err error
		// We seed the three length constraints with a sentinel value.
		length    = -1
		minLength = -1
		maxLength = -1
		base      = "string"
	)
	dtProps := map[string]any{}

	valueConstraintNames := []string{"minInclusive", "maxInclusive", "minExclusive", "maxExclusive"}
	valueConstraints := make(map[string]stringAndAny, 4)
	for _, v := range valueConstraintNames {
		valueConstraints[v] = stringAndAny{"", nil}
	}

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
			for _, v := range valueConstraintNames {
				s2a = valueConstraints[v]
				if s2a.str == "" {
					s2a.str, err = jsonutil.GetString(dtProps, v, "")
					if err != nil {
						return nil, err
					}
				}
				valueConstraints[v] = s2a
			}
			// minimum is just an alias for minInclusive.
			if valueConstraints["minInclusive"].str == "" {
				s2a = valueConstraints["minInclusive"]
				s2a.str, err = jsonutil.GetString(dtProps, "minimum", "")
				if err != nil {
					return nil, err
				}
				valueConstraints["minInclusive"] = s2a
			}
			// and so is maximum
			if valueConstraints["maxInclusive"].str == "" {
				s2a = valueConstraints["maxInclusive"]
				s2a.str, err = jsonutil.GetString(dtProps, "maximum", "")
				if err != nil {
					return nil, err
				}
				valueConstraints["maxInclusive"] = s2a
			}
		}
	}
	// Now convert the constraints appropriately:
	err = baseTypes[base].setValueConstraints(valueConstraints)
	if err != nil {
		return nil, err
	}
	// We compute the derived description map once at instantiation.
	dd, err := baseTypes[base].getDerivedDescription(dtProps)
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
		MinInclusive:       valueConstraints["minInclusive"].val,
		MinExclusive:       valueConstraints["minExclusive"].val,
		MaxInclusive:       valueConstraints["maxInclusive"].val,
		MaxExclusive:       valueConstraints["maxExclusive"].val,
	}
	return res, nil
}

func (dt *Datatype) ToString(val any) (string, error) {
	return baseTypes[dt.Base].toString(dt, val)
}

func (dt *Datatype) ToGo(s string, noChecks bool) (any, error) {
	return baseTypes[dt.Base].toGo(dt, s, noChecks)
}

func (dt *Datatype) SqlType() string {
	return baseTypes[dt.Base].sqlType
}

func (dt *Datatype) ToSql(val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	return baseTypes[dt.Base].toSql(dt, val)
}
