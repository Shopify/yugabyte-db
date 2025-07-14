.PHONY: all build test clean proto install-tools

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_CLIENT=yb-client
BINARY_SERVER=yb-server

all: build

build: proto
	$(GOBUILD) -o $(BINARY_CLIENT) cmd/yb-client/main.go
	$(GOBUILD) -o $(BINARY_SERVER) cmd/yb-server/main.go

install-tools:
	@echo "Installing protoc-gen-go..."
	$(GOCMD) install google.golang.org/protobuf/cmd/protoc-gen-go@latest

proto:
	@echo "Generating Go code from proto files..."
	@if ! command -v protoc-gen-go >/dev/null 2>&1; then \
		echo "protoc-gen-go not found, installing..."; \
		make install-tools; \
	fi
	PATH=$$PATH:$$(go env GOPATH)/bin protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		pb/*.proto

test:
	$(GOTEST) -v ./...

clean:
	rm -f $(BINARY_CLIENT) $(BINARY_SERVER)
	rm -f pb/*.pb.go

deps:
	$(GOMOD) download
	$(GOMOD) tidy

run-client: build
	./$(BINARY_CLIENT)

run-server: build
	./$(BINARY_SERVER)

# Development helpers
fmt:
	$(GOCMD) fmt ./...

vet:
	$(GOCMD) vet ./...

lint: fmt vet