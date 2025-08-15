GO      ?= go
BIN     := bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build run sim test test-race cover bench vet fmt lint tidy docker compose-up compose-down clean

all: fmt vet test build

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/brokerd ./cmd/brokerd
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/erpsim ./cmd/erpsim

run: build
	$(BIN)/brokerd -config config/broker.yaml

sim: build
	$(BIN)/erpsim -events 500 -failure-rate 0.15

test:
	$(GO) test ./...

test-race:
	$(GO) test -race -count=1 ./...

cover:
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -func=coverage.out | tail -1
	$(GO) tool cover -html=coverage.out -o coverage.html

bench:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

docker:
	docker build --build-arg VERSION=$(VERSION) -t rabbitmq-lite:$(VERSION) .

compose-up:
	docker compose -f deployments/docker-compose.yml up --build -d

compose-down:
	docker compose -f deployments/docker-compose.yml down -v

clean:
	rm -rf $(BIN) coverage.out coverage.html data
