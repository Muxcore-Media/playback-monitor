.PHONY: build test lint clean fmt tidy ci proto

GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "0.0.0-dev")
LDFLAGS ?= -s -w
BINARY ?= playback-monitor

proto:
	PATH="$(HOME)/go/bin:$$PATH" protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/monitorv1/monitor.proto

build:
	$(GO) build -ldflags="$(LDFLAGS)" -o $(BINARY) ./cmd/module

test:
	$(GO) test -race -count=1 -timeout 60s ./...

lint:
	golangci-lint run --timeout 120s ./...

clean:
	rm -f $(BINARY)

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

ci: lint test build
