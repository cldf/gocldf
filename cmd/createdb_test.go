package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestCreatedb(t *testing.T) {
	dir := t.TempDir()
	actual := new(bytes.Buffer)
	rootCmd.SetOut(actual)
	rootCmd.SetErr(actual)
	rootCmd.SetArgs([]string{"createdb", "../csvw/dataset/testdata/StructureDataset-metadata.json", dir + "/test.sqlite"})
	rootCmd.Execute()

	expected := `Bengali`
	if !strings.Contains(actual.String(), expected) {
		t.Errorf(`problem: "%q"" not in "%q""`, expected, actual.String())
	}
}
