# GoStoreLog

A persistent, sequential, immutable storage engine for Go, designed with clean architecture.

## Features

- **Persistent Storage**: Data is stored on disk in `.store` and `.index` files.
- **Sequential Ordering**: Records are appended in order, with immutable offsets.
- **Data Types**: Supports JSON, bytes, and strings.
- **Partitioning**: Data can be partitioned by user-defined keys.
- **Auto-Segmentation**: Files are segmented when reaching max size (e.g., 10MB).
- **Atomic Appends**: Ensures data safety during writes.
- **HTTP API**: RESTful API for publishing and reading records with panic recovery middleware, including replication endpoint.
- **Client SDK**: Go client for interacting with the storage engine.
- **Pub/Sub Integration**: Uses a generic connector for message handling (currently Go channels, extensible to Kafka, etc.) with panic recovery.
- **Recovery**: Automatically loads existing data on startup.
- **Graceful Shutdown**: Ensures data is persisted before shutdown.
- **Consistency Checks**: Sanity checks after appends and automatic repair for store/index inconsistencies.
- **Retry Mechanism**: Retries index writes on failure.
- **Multi-Node Clustering**: Leader election via Redis with Raft consensus fallback, gossip protocol for node discovery, DNS-based address resolution.
- **Data Replication**: Leader replicates data to followers via HTTP push for consistency across nodes.
- **Gap Detection**: Leader periodically checks data gaps with followers and stores gap information for monitoring and reconciliation.
- **Fault Tolerance**: Automatically switches to Raft consensus if Redis is unavailable, ensuring leader election without external dependencies.
- **Clean Architecture**: Organized into entity, repository, usecase, handler, cluster layers.

## Architecture

- `internal/entity`: Data models and business entities.
- `internal/repository`: Data access layer for storage.
- `internal/usecase`: Business logic.
- `internal/handler`: HTTP handlers and pub/sub connectors.
- `pkg/client`: Client SDK for external interactions.
- `pkg`: Generic utilities like logging.
- `cmd`: Application entry point.

## Usage

1. Start the server:
   ```bash
   go run cmd/main.go
   ```
   The server listens on `:8080` for HTTP requests.

2. Use the Client SDK to publish and read:
   ```go
   client := client.NewClient("http://localhost:8080")
   err := client.Publish(map[string]string{"key": "value"}, 0, "partition1") // 0 for JSON
   record, err := client.Read("partition1", 0)
   ```

### API Endpoints

- `POST /publish`: Publish a record. Body: `{"data": <data>, "data_type": <int>, "partition_key": <string>}`
- `GET /read?partition=<key>&offset=<offset>`: Read a record by partition and offset.
- `POST /replicate`: Receive replicated data from leader. Body: `{"data": <data>, "data_type": <int>, "partition_key": <string>}`
- `GET /status`: Get node status for gap detection.
- `GET /gaps`: Query stored gap information between leader and followers.

Data types: 0=JSON, 1=Bytes, 2=String.

## Clustering

GoStoreLog supports multi-node clustering with leader election, gossip-based discovery, and data replication:

- **Leader Election**: Uses Redis for distributed locking to elect a leader node.
- **Node Discovery**: Nodes use DNS resolution to find initial peers and gossip protocol for ongoing discovery.
- **Data Replication**: Leader pushes new records to followers via HTTP for data consistency.
- **Example**: With nodes A (leader), B, C:
  - A holds the leader lock in Redis.
  - B and C resolve A's address via DNS and join the gossip cluster.
  - When data is stored on A, it is automatically pushed to B and C via HTTP.
  - All nodes maintain consistent data through replication.

Set environment variables for cluster configuration. The leader node coordinates cluster activities and replication, while followers can be promoted if the leader fails.

## Configuration

- `DataDir`: Directory for data files (default: `./data`).
- `MaxFileSize`: Max size per segment in bytes (default: 10MB).

### Clustering Configuration (Environment Variables)
- `NODE_ID`: Unique identifier for this node (default: `node1`).
- `BIND_ADDR`: Address to bind the gossip listener (default: `0.0.0.0:7946`).
- `ADVERTISE_ADDR`: Address to advertise to other nodes (default: `127.0.0.1:7946`).
- `REDIS_ADDR`: Redis server address for leader election (default: `localhost:6379`).
- `SERVICE_NAME`: DNS service name for node discovery (default: `gostorelog-cluster`).
- `CLUSTER_PORT`: Port for cluster communication (default: `7946`).
- `DATA_DIR`: Directory for data files (default: `./data`).

## Testing

Run tests:
```bash
go test ./...
```

Run benchmarks:
```bash
go test -bench=. ./internal/repository/
```

Test data is stored in the `test-data/` directory in the project root for easy inspection and maintenance. Unit tests clean up previous data at the start but leave files after completion for sanity checks. Segmentation tests create multiple segments and dump binary files to human-readable `.txt` versions. End-to-end tests include consistency checks and repair mechanisms. Cluster tests log detailed scenarios for leader election, DNS resolution, and node promotion.

## Future Enhancements

- Data replication across cluster nodes.
- Additional connectors (Kafka, Flink).
- Compression, encryption.
- Query capabilities.