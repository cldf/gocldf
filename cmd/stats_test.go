package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func Test_ExecuteStats(t *testing.T) {
	actual := new(bytes.Buffer)
	rootCmd.SetOut(actual)
	rootCmd.SetErr(actual)
	rootCmd.SetArgs([]string{"stats", "../csvw/dataset/testdata/StructureDataset-metadata.json"})
	rootCmd.Execute()

	expected := `LanguageTable`
	if !strings.Contains(actual.String(), expected) {
		t.Errorf(`problem: "%q"" not in "%q""`, expected, actual.String())
	}
	rootCmd.SetArgs([]string{"stats", "../csvw/dataset/testdata/StructureDataset-metadata.json", "--metadata"})
	rootCmd.Execute()

	expected = `dc:description`
	if !strings.Contains(actual.String(), expected) {
		t.Errorf(`problem: "%q"" not in "%q""`, expected, actual.String())
	}
}
