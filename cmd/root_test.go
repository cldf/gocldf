package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func Test_ExecuteRoot(t *testing.T) {
	actual := new(bytes.Buffer)
	rootCmd.SetOut(actual)
	rootCmd.SetErr(actual)
	rootCmd.SetArgs([]string{"-h"})
	rootCmd.Execute()

	Execute()

	expected := `createdb`
	if !strings.Contains(actual.String(), expected) {
		t.Errorf(`problem: "%q"" not in "%q""`, expected, actual.String())
	}

}
