# Shopify YugabyteDB Go Client

A Go implementation of the YugabyteDB RPC protocol client that can call ListTabletServersRequest.

## Overview

This client implements the YugabyteDB proprietary RPC protocol, which:
- Uses "YB\x01" as the connection header magic bytes
- Employs Protocol Buffers for message serialization
- Implements request/response framing with length-prefixed messages
- Supports connection context establishment

## Building

```bash
cd shopify-yb-client
go mod tidy

# Build CLI client
go build -o yb-client cmd/yb-client/main.go

# Build HTTP server
go build -o yb-server cmd/yb-server/main.go
```

## Usage

### CLI Client

```bash
# Connect to local YugabyteDB master (default port 7100)
./yb-client

# Connect to specific master address
./yb-client -master hostname:7100

# Show help
./yb-client -help
```

### HTTP Server

```bash
# Start server on default port 8080
./yb-server

# Specify custom ports
./yb-server -listen :9090 -master hostname:7100

# Show help
./yb-server -help
```

#### API Endpoints

**GET /api/v1/tablet-servers**

Returns a JSON list of all tablet servers in the cluster.

Example response:
```json
{
  "servers": [
    {
      "uuid": "6534653835393532643664653464343862333132303266396437373364353335",
      "sequence_number": 1751983364071700,
      "rpc_address": "10.0.1.10:9100",
      "placement_cloud": "aws",
      "placement_region": "us-west-2",
      "placement_zone": "us-west-2a",
      "alive": true,
      "millis_since_heartbeat": 258,
      "last_heartbeat": "2025-01-14T10:50:07.123Z"
    }
  ],
  "count": 3
}
```

**GET /api/v1/masters**

Returns a JSON list of all master servers in the cluster.

Example response:
```json
{
  "masters": [
    {
      "uuid": "abc123def456",
      "rpc_address": "10.0.1.1:7100",
      "http_address": "10.0.1.1:7000",
      "role": "LEADER",
      "cloud": "aws",
      "region": "us-west-2",
      "zone": "us-west-2a"
    }
  ],
  "count": 3
}
```

**GET /api/v1/cluster-config**

Returns the cluster configuration.

Example response:
```json
{
  "config": {
    "version": 1,
    "cluster_uuid": "cluster-uuid-12345",
    "server_version": "2.17.1.0",
    "replication_info": {
      "live_replicas": {
        "num_replicas": 3
      },
      "affinitized_leaders": ["us-west-2a", "us-west-2b"]
    },
    "encryption_enabled": false
  }
}
```

Error response (for all endpoints):
```json
{
  "error": "Failed to connect to master: connection refused"
}
```

## Architecture

The client consists of:

1. **Protocol Buffers** (`pb/`): Minimal proto definitions for RPC headers and ListTabletServers messages
2. **RPC Client** (`pkg/rpc/client.go`): Core RPC protocol implementation
3. **Main Program** (`cmd/yb-client/main.go`): CLI that calls ListTabletServersRequest

## Protocol Details

The YugabyteDB RPC protocol follows this structure:

### Connection Setup
1. Send 3-byte header: "YB\x01"
2. Send ConnectionContext message with call_id=-3

### Message Format
```
[4 bytes: total_size (big-endian)]
[varint: header_size]
[header_size bytes: RequestHeader protobuf]
[varint: body_size]
[body_size bytes: Request protobuf]
```

### Response Format
```
[4 bytes: total_size (big-endian)]
[varint: header_size]
[header_size bytes: ResponseHeader protobuf]
[varint: body_size]
[body_size bytes: Response protobuf or ErrorStatusPB]
```
