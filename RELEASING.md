# Releasing

go mod tidy

go test ./...

~/venvs/cldf/bin/python test/test_regression.py

FIXME: get cross-compilation set up for goreleaser (e.g. )

goreleaser check
goreleaser release --snapshot --clean
