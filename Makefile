GOBIN ?= $(shell go env GOPATH)/bin
MOCKGEN := $(GOBIN)/uber-mockgen
GOTESTSUM := $(GOBIN)/gotestsum

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

$(MOCKGEN):
	@echo ">> getting go.uber.org/mock/mockgen and its dependencies..."
	@go get go.uber.org/mock/mockgen@latest

$(GOTESTSUM):
	@echo ">> getting gotest.tools/gotestsum and its dependencies..."
	@go get gotest.tools/gotestsum@latest

.PHONY: tidy
tidy:
	@echo ">> tidying Go modules..."
	@go mod tidy
