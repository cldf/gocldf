package csvw

import (
	"fmt"
	"gocldf/csvw/datatype"
	"gocldf/internal/jsonutil"
	"slices"
	"strconv"
	"strings"
)

type Column struct {
	Name          string
	CanonicalName string // Either the CLDF property short name or the column name
	PropertyUrl   string
	Datatype      datatype.Datatype
	Separator     string
	Null          []string
}

func NewColumn(index int, jsonCol map[string]interface{}) (*Column, error) {
	var (
		err           error
		name          = ""
		purl          = ""
		sep           = ""
		canonicalName = ""
	)
	name, err = jsonutil.GetString(jsonCol, "name", "Col_"+strconv.Itoa(index+1))
	if err != nil {
		return nil, err
	}
	sep, err = jsonutil.GetString(jsonCol, "separator", "")
	if err != nil {
		return nil, err
	}
	purl, err = jsonutil.GetString(jsonCol, "propertyUrl", "")
	if err != nil {
		return nil, err
	}
	null, err := jsonutil.GetStringArray(jsonCol, "null")
	if err != nil {
		return nil, err
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
	dt, err := datatype.New(jsonCol)
	if err != nil {
		return nil, err
	}
	col := &Column{
		Name:          name,
		CanonicalName: canonicalName,
		PropertyUrl:   purl,
		Datatype:      *dt,
		Separator:     sep,
		Null:          null}
	return col, nil
}

func (column *Column) ToGo(s string, split bool, noChecks bool) (any, error) {
	if slices.Contains(column.Null, s) {
		// Return an empty list for list-valued fields, nil otherwise.
		if split && column.Separator != "" {
			return make([]string, 0), nil
		}
		return nil, nil
	}
	if split && column.Separator != "" {
		fields := strings.Split(s, column.Separator)
		res := make([]string, len(fields))
		for i, field := range fields {
			val, err := column.ToGo(field, false, noChecks)
			if err != nil {
				return nil, err
			}
			res[i] = val.(string)
		}
		return res, nil
	}
	return column.Datatype.ToGo(s, noChecks)
}

func (column *Column) ToSql(x any) (any, error) {
	return column.Datatype.ToSql(x)
}

func (column *Column) ToString(x any) (string, error) {
	if x == nil {
		return column.Null[0], nil
	}
	return column.Datatype.ToString(x)
}

func (column *Column) sqlCreate(noChecks bool) string {
	res := fmt.Sprintf("`%v`\t%v", column.CanonicalName, column.Datatype.SqlType())
	if !noChecks {
		var checks []string
		if column.Datatype.MinInclusive != nil {
			checks = append(checks, fmt.Sprintf("`%v` >= %v", column.CanonicalName, column.Datatype.MinInclusive))
		}
		if column.Datatype.MinExclusive != nil {
			checks = append(checks, fmt.Sprintf("`%v` > %v", column.CanonicalName, column.Datatype.MinExclusive))
		}
		if column.Datatype.MaxInclusive != nil {
			checks = append(checks, fmt.Sprintf("`%v` <= %v", column.CanonicalName, column.Datatype.MaxInclusive))
		}
		if column.Datatype.MaxExclusive != nil {
			checks = append(checks, fmt.Sprintf("`%v` < %v", column.CanonicalName, column.Datatype.MaxExclusive))
		}
		if column.Datatype.Length >= 0 {
			checks = append(checks, fmt.Sprintf("length(`%v`) = %v", column.CanonicalName, column.Datatype.Length))
		}
		if column.Datatype.MinLength >= 0 {
			checks = append(checks, fmt.Sprintf("length(`%v`) >= %v", column.CanonicalName, column.Datatype.MinLength))
		}
		if column.Datatype.MaxLength >= 0 {
			checks = append(checks, fmt.Sprintf("length(`%v`) <= %v", column.CanonicalName, column.Datatype.MaxLength))
		}
		if len(checks) > 0 {
			res += fmt.Sprintf(" CHECK(%v)", strings.Join(checks, " AND "))
		}
	}
	return res
}
