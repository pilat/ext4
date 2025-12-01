.PHONY: test lint fmt vet clean check

test:
	go test -v ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	}
	@golangci-lint run --exclude-files e2e_test.go,tmp/

fmt:
	@go fmt ./...

vet:
	@go vet ./...

check: fmt vet lint test
