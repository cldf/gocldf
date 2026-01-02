package cldf

import (
	"bufio"
	"fmt"
	"gocldf/internal/pathutil"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/nickng/bibtex"
)

var FieldsBiblatex = []string{
	"abstract",
	"author",
	"booktitle",
	"date",
	"doi",
	"editor",
	"entrysubtype",
	"eprint",
	"isbn",
	"issn",
	"journaltitle",
	"langid",
	"location",
	"note",
	"number",
	"pages",
	"publisher",
	"shorttitle",
	"title",
	"url",
	"urldate",
	"usera",
	"userb",
	"userc",
	"userd",
	"usere",
	"userf",
	"verba",
	"verbb",
	"verbc",
	"volume",
}
var FieldsBibtex = []string{
	"address",
	"annote",
	"author",
	"booktitle",
	"chapter",
	"crossref",
	"edition",
	"editor",
	"howpublished",
	"institution",
	"journal",
	"key",
	"month",
	"note",
	"number",
	"organization",
	"pages",
	"publisher",
	"school",
	"series",
	"title",
	"type",
	"volume",
	"year",
}
var BibtexFieldsets = map[string][]string{
	"biblatex": FieldsBiblatex,
	"bibtex":   FieldsBibtex,
}

type Source struct {
	Id     string
	Type   string
	Fields map[string]string
}

func NewSource(entry *bibtex.BibEntry, allowedFields map[string]struct{}) *Source {
	fields := make(map[string]string)
	for k, v := range entry.Fields {
		if len(allowedFields) > 0 {
			_, ok := allowedFields[k]
			if !ok {
				continue
			}
		}
		// We reverse the temporary replacement for @ to appease the BibTeX parser.
		fields[k] = strings.ReplaceAll(v.String(), "�", "@")
	}
	return &Source{
		Id:     entry.CiteName,
		Type:   entry.Type,
		Fields: fields,
	}
}

type Sources struct {
	Path       string
	Items      []*Source
	FieldNames []string
}

func normalizeBibtex(r io.Reader) (io.Reader, error) {
	var res []string
	comment := regexp.MustCompile("^\\s*comment\\s*=")
	atAtStart := regexp.MustCompile("^\\s*@")
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if comment.MatchString(line) {
			// For some reason "comment" seems to be forbidden as field name.
			line = strings.Replace(line, "comment", "comments", 1)
		}
		if !atAtStart.MatchString(line) {
			line = strings.ReplaceAll(line, "@", "�")
		}
		res = append(res, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return strings.NewReader(strings.Join(res, "\n")), nil
}

func NewSources(p string, fieldsets ...string) (sources *Sources, err error) {
	pp, f, err := pathutil.Reader(p)
	if err != nil {
		return nil, err
	}
	defer func(file any) {
		switch file.(type) {
		case *os.File:
			err = file.(*os.File).Close()
		}
	}(f)

	allowedFields := make(map[string]struct{})
	for _, fieldset := range fieldsets {
		for _, field := range BibtexFieldsets[fieldset] {
			allowedFields[field] = struct{}{}
		}
	}

	r, err := normalizeBibtex(f.(io.Reader))
	if err != nil {
		return nil, err
	}
	entries, err := bibtex.Parse(r)
	if err != nil {
		return nil, fmt.Errorf("error parsing %v: %w", p, err)
	}
	res := make([]*Source, len(entries.Entries))
	var fields []string
	for i, entry := range entries.Entries {
		res[i] = NewSource(entry, allowedFields)
		for name := range res[i].Fields {
			if !slices.Contains(fields, name) {
				fields = append(fields, name)
			}
		}
	}
	return &Sources{Path: pp, Items: res, FieldNames: fields}, nil
}

func (s *Sources) SqlCreate() string {
	var res []string
	res = append(res, "CREATE TABLE IF NOT EXISTS `SourceTable` (")
	res = append(res, "\t`id`\tTEXT,")
	res = append(res, "\t`genre`\tTEXT,")
	for _, field := range s.FieldNames {
		if field == "type" || field == "id" {
			field += "_"
		}
		res = append(res, fmt.Sprintf("\t`%s`\tTEXT,", field))
	}
	res = append(res, "\tPRIMARY KEY(`id`)")
	res = append(res, ");")
	return strings.Join(res, "\n")
}

func (s *Sources) itemsToSql() (rows [][]any, colNames []string, err error) {
	colNames = []string{}
	rows = make([][]any, len(s.Items))
	for i, item := range s.Items {
		if i == 0 {
			colNames = append(colNames, "id", "genre")
		}
		rows[i] = make([]any, len(s.FieldNames)+2)
		rows[i][0] = item.Id
		rows[i][1] = item.Type

		for j, field := range s.FieldNames {
			if field == "type" || field == "id" {
				field += "_"
			}
			if i == 0 {
				colNames = append(colNames, field)
			}
			val, ok := item.Fields[field]
			if !ok {
				val = ""
			}
			rows[i][j+2] = val
		}
	}
	return rows, colNames, nil
}
