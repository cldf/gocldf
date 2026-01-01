package pathutil

import (
	"io"
	"path/filepath"
	"testing"
)

func read(fname string) (string, error) {
	r, err := Reader(filepath.Join("testdata", fname))
	if err != nil {
		return "", err
	}
	b, err := io.ReadAll(r.(io.Reader))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func Test_Reader(t *testing.T) {
	expected := "hello world!\n"
	res, _ := read("test.txt")
	if res != expected {
		t.Errorf(`problem: "%v" vs "%v"`, expected, res)
	}
	res, _ = read("test2.txt")
	if res != expected {
		t.Errorf(`problem: "%v" vs "%v"`, expected, res)
	}
	res, err := read("test3.txt")
	if err == nil {
		t.Errorf(`problem: "%v" vs "%v"`, expected, res)
	}
}
