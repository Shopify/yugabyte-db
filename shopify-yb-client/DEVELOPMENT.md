# Development Guide

## Prerequisites

- Go 1.21 or later
- Protocol Buffers compiler (`protoc`) - [Installation guide](https://grpc.io/docs/protoc-installation/)
- `protoc-gen-go` plugin (will be installed automatically by Makefile)

## Generating Protocol Buffers

The project uses Protocol Buffers for RPC message definitions. The `.proto` files are located in the `pb/` directory.

### Automatic Generation

Use the Makefile to generate Go code from proto files:

```bash
# Generate proto files (installs protoc-gen-go if needed)
make proto

# Or as part of the build process
make build
```

### Manual Generation

If you prefer to generate manually:

```bash
# Install the protoc Go plugin
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# Generate Go code from proto files
PATH=$PATH:$(go env GOPATH)/bin protoc \
    --go_out=. \
    --go_opt=paths=source_relative \
    pb/*.proto
```

### Adding New Proto Definitions

1. Create or modify `.proto` files in the `pb/` directory
2. Follow the existing naming conventions and package structure
3. Set the Go package option: `option go_package = "github.com/Shopify/yugabyte-db/shopify-yb-client/pb;pb";`
4. Run `make proto` to generate the Go code
5. The generated `*.pb.go` files will appear alongside the proto files

## Building

```bash
# Build everything
make build

go build -o yb-server cmd/yb-server/main.go

# Clean build artifacts
make clean
```

## Testing

```bash
# Run all tests
make test

# Run tests with coverage
go test -v -cover ./...

# Run specific package tests
go test -v ./pkg/rpc
```

## Code Quality

```bash
# Format code
make fmt

# Run go vet
make vet

# Run both fmt and vet
make lint
```

## Project Structure

```
shopify-yb-client/
├── cmd/
│   └── yb-server/    # HTTP server
├── pkg/
│   └── rpc/          # RPC client implementation
├── pb/               # Protocol buffer definitions
├── Makefile          # Build automation
├── go.mod            # Go module definition
└── README.md         # User documentation
```

## Adding New RPC Methods

1. Add the proto definition to `pb/master_cluster.proto` or create a new `.proto` file
2. Run `make proto` to generate Go code
3. Add a new method to the HTTP server in `cmd/yb-server/main.go`
4. Create handler function following the pattern of `handleTabletServers`
5. Register the new endpoint in the `main()` function

Example:
```go
// In main()
http.HandleFunc("/api/v1/masters", handleListMasters)

// Handler function
func handleListMasters(w http.ResponseWriter, r *http.Request) {
    // Implementation similar to handleTabletServers
}
```

## Debugging

To enable verbose logging in the RPC client, you can add debug print statements in `pkg/rpc/client.go`. The client already suppresses expected connection closure errors.

Common issues:
- Wrong port (7000 is web UI, 7100 is RPC)
- YugabyteDB not running
- Network connectivity issues
- Proto field type mismatches (check required vs optional)
