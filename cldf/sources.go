package cldf

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/nickng/bibtex"
)

type Source struct {
	Id     string
	Type   string
	Fields map[string]string
}

func NewSource(entry *bibtex.BibEntry) *Source {
	fields := make(map[string]string)
	for k, v := range entry.Fields {
		fields[k] = v.String()
	}
	return &Source{
		Id:     entry.CiteName,
		Type:   entry.Type,
		Fields: fields,
	}
}

type Sources struct {
	Items      []*Source
	FieldNames []string
}

func NewSources(p string) (*Sources, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	entries, err := bibtex.Parse(f)
	if err != nil {
		return nil, err
	}
	res := make([]*Source, len(entries.Entries))
	fields := []string{}
	for i, entry := range entries.Entries {
		res[i] = NewSource(entry)
		for name, _ := range entry.Fields {
			if !slices.Contains(fields, name) {
				fields = append(fields, name)
			}
		}
	}
	return &Sources{Items: res, FieldNames: fields}, nil

}

func (s *Sources) SqlCreate() string {
	res := []string{}
	res = append(res, "CREATE TABLE IF NOT EXISTS `SourceTable` (")
	res = append(res, "\t`id`\tTEXT,")
	res = append(res, "\t`genre`\tTEXT,")
	for _, field := range s.FieldNames {
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
