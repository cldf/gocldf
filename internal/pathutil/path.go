package pathutil

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

func GetFormattedSize(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	// Size() returns the size in bytes as an int64
	units := []string{"bytes", "KB", "MB", "GB"}
	size := float64(info.Size())

	for _, unit := range units {
		if size < 1024.0 && size > -1024.0 {
			return fmt.Sprintf("%.1f%v", size, unit), nil
		}
		size = size / 1024.0
	}
	return fmt.Sprintf("%g%v", size, "TB"), nil
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true // Path exists
	}
	if errors.Is(err, os.ErrNotExist) {
		return false // Specifically does not exist
	}
	return false
}

func GetFreshPath(path string, overwrite bool) (p string, err error) {
	if PathExists(path) {
		if overwrite {
			err = os.Remove(path)
			if err != nil {
				return "", err
			}
		} else {
			return "", errors.New("path already exists")
		}
	}
	return path, nil
}

func readZipped(fp string) (bytes []byte, err error) {
	r, err := zip.OpenReader(fp)
	if err != nil {
		return nil, err
	}
	defer func(r *zip.ReadCloser) {
		err = r.Close()
	}(r)

	var contentBytes []byte
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		contentBytes, err = io.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		err = rc.Close()
		if err != nil {
			return nil, err
		} // Must close each file reader individually
		break
	}
	return contentBytes, nil
}

/*
Reader may return an opened file which must be closed by the caller.

Usage:

	reader, err := pathutil.Reader(p)
	if err != nil {}
	defer func(r any) {
		switch r.(type) {
		case *os.File:
			err = r.(*os.File).Close()
		}
	}(reader)
*/
func Reader(p string) (pp string, r any, err error) {
	if !PathExists(p) {
		zippedBytes, err := readZipped(p + ".zip")
		if err != nil {
			return "", nil, err
		}
		return p + ".zip", bytes.NewReader(zippedBytes), nil
	}
	file, err := os.Open(p)
	if err != nil {
		return "", nil, err
	}
	return p, file, nil
}
