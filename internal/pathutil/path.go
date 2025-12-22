package pathutil

import (
	"errors"
	"os"
)

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
