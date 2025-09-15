PKG_DIR := ./pkg/store
MOCK_DIR := $(PKG_DIR)/mocks

.PHONY: mocks
mocks: $(MOCKGEN)
	@echo ">> generating mocks..."
	@go generate $(PKG_DIR)

.PHONY: test
test:
	@echo ">> running tests..."
	@MallocNanoZone=0 go run gotest.tools/gotestsum  -- -race ./...

.PHONY: lint
lint:
	@echo ">> running golangci-lint..."
	@go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.2 run ./...

.PHONY: tidy
tidy:
	@echo ">> tidying Go modules..."
	@go mod tidy

.PHONY: imports
imports:
	@echo ">> fixing import order with gci..."
	@go run github.com/daixiang0/gci write --skip-generated -s standard -s default -s "Prefix(github.com/castai)" ./
