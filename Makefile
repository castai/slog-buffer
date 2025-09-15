GOBIN ?= $(shell go env GOPATH)/bin
MOCKGEN := $(GOBIN)/uber-mockgen
GOTESTSUM := $(GOBIN)/gotestsum
GOLANGCI_LINT := $(GOBIN)/golangci-lint
GCI := $(GOBIN)/gci

PKG_DIR := ./pkg/store
MOCK_DIR := $(PKG_DIR)/mocks

.PHONY: mocks
mocks: $(MOCKGEN)
	@echo ">> generating mocks..."
	@go generate $(PKG_DIR)

.PHONY: test
test: $(GOTESTSUM)
	@echo ">> running tests..."
	@MallocNanoZone=0 CGO_ENABLED=0 $(GOTESTSUM) -- -race ./...

.PHONY: lint
lint: $(GOLANGCI_LINT)
	@echo ">> running golangci-lint..."
	@$(GOLANGCI_LINT) run ./...

.PHONY: tidy
tidy:
	@echo ">> tidying Go modules..."
	@go mod tidy

.PHONY: imports
imports: $(GCI)
	@echo ">> fixing import order with gci..."
	@$(GCI) write --skip-generated -s standard -s default -s "Prefix(github.com/castai)" ./

$(MOCKGEN):
	@echo ">> getting go.uber.org/mock/mockgen and its dependencies..."
	@go get go.uber.org/mock/mockgen@latest

$(GOTESTSUM):
	@echo ">> getting gotest.tools/gotestsum and its dependencies..."
	@go get gotest.tools/gotestsum@latest

$(GOLANGCI_LINT):
	@echo ">> installing golangci-lint..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.2

$(GCI):
	@echo ">> installing gci..."
	@go install github.com/daixiang0/gci@latest
