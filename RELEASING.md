# Releasing

go mod tidy

go test ./...

goreleaser check
goreleaser release --snapshot --clean
