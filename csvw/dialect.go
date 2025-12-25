/*
Limitations:
  - go's csv.Reader always skips blank rows, see https://github.com/golang/go/issues/39119
    so the default (false) for skipBlankRows cannot be implemented currently.
*/
package csvw

import (
	"encoding/csv"
	"errors"
	"gocldf/internal/jsonutil"
	"slices"
)

type Dialect struct {
	commentPrefix rune // Reader.Comment rune
	delimiter     rune // Reader.Comma rune

	// CLDF requires UTF-8 encoded files
	//encoding        string
	header          bool
	headerRowCount  int
	lineTerminators []string
	quoteChar       string

	// go's csv.Reader always skips blank rows https://github.com/golang/go/issues/39119
	skipBlankRows    bool
	skipColumns      int
	skipInitialSpace bool // trimString = "start"

	skipRows int

	doubleQuote bool
	trim        string
}

func NewDialect(jsonTableOrTableGroup map[string]any) (*Dialect, error) {
	res := &Dialect{
		commentPrefix:    rune(0),
		delimiter:        []rune(",")[0],
		header:           true,
		headerRowCount:   1,
		lineTerminators:  []string{"\r\n", "\n"}, // not yet supported
		quoteChar:        `"`,                    // not yet supported
		skipBlankRows:    false,
		skipColumns:      0, // not yet supported
		skipInitialSpace: false,
		skipRows:         0,    // not yet supported
		doubleQuote:      true, // false not yet supported
		trim:             "false",
	}
	jsonDialect, ok := jsonTableOrTableGroup["dialect"]
	if !ok {
		return res, nil
	}
	val, err := jsonutil.GetRune(jsonDialect.(map[string]any), "commentPrefix", rune(0))
	if err != nil {
		return nil, err
	}
	res.commentPrefix = val
	val, err = jsonutil.GetRune(jsonDialect.(map[string]any), "delimiter", rune(','))
	if err != nil {
		return nil, err
	}
	res.delimiter = val
	header, err := jsonutil.GetBool(jsonDialect.(map[string]any), "header", true)
	if err != nil {
		return nil, err
	}
	res.header = header
	skipSpace, err := jsonutil.GetBool(jsonDialect.(map[string]any), "skipInitialSpace", false)
	if err != nil {
		return nil, err
	}
	trim, ok := jsonDialect.(map[string]any)["trim"]
	if ok { // Explicit trim flag, we ignore skipInitialSpace.
		trimBool, ok := trim.(bool)
		if ok {
			if trimBool {
				res.trim = "true"
			} else {
				res.trim = "false"
			}
		} else {
			trimString, ok := trim.(string)
			if ok {
				valid := []string{"true", "false", "start", "end"}
				if !slices.Contains(valid, trimString) {
					return nil, errors.New("invalid 'trim' format")
				}
				res.trim = trimString
			} else {
				return nil, errors.New("invalid 'trim' format")
			}
		}
	} else if skipSpace { // No trim flag, so we honor skipInitialSpace.
		res.trim = "start"
	}
	return res, nil
}

func (d *Dialect) ConfigureCsvReader(reader *csv.Reader) {
	// Comma is the field delimiter.
	// It is set to comma (',') by NewReader.
	// Comma must be a valid rune and must not be \r, \n,
	// or the Unicode replacement character (0xFFFD).
	reader.Comma = d.delimiter

	// Comment, if not 0, is the comment character. Lines beginning with the
	// Comment character without preceding whitespace are ignored.
	// With leading whitespace the Comment character becomes part of the
	// field, even if TrimLeadingSpace is true.
	// Comment must be a valid rune and must not be \r, \n,
	// or the Unicode replacement character (0xFFFD).
	// It must also not be equal to Comma.
	if d.commentPrefix != rune(0) {
		reader.Comment = d.commentPrefix
	}

	// If TrimLeadingSpace is true, leading white space in a field is ignored.
	// This is done even if the field delimiter, Comma, is white space.
	if d.trim == "start" {
		reader.TrimLeadingSpace = true
	}
}
