# GoStoreLog

A persistent, sequential, immutable storage engine for Go, designed with clean architecture.

## Features

- **Persistent Storage**: Data is stored on disk in `.store` and `.index` files.
- **Sequential Ordering**: Records are appended in order, with immutable offsets.
- **Data Types**: Supports JSON, bytes, and strings.
- **Partitioning**: Data can be partitioned by user-defined keys.
- **Auto-Segmentation**: Files are segmented when reaching max size (e.g., 10MB).
- **Atomic Appends**: Ensures data safety during writes.
- **HTTP API**: RESTful API for publishing and reading records with panic recovery middleware.
- **Client SDK**: Go client for interacting with the storage engine.
- **Pub/Sub Integration**: Uses a generic connector for message handling (currently Go channels, extensible to Kafka, etc.) with panic recovery.
- **Recovery**: Automatically loads existing data on startup.
- **Graceful Shutdown**: Ensures data is persisted before shutdown.
- **Consistency Checks**: Sanity checks after appends and automatic repair for store/index inconsistencies.
- **Retry Mechanism**: Retries index writes on failure.
- **Clean Architecture**: Organized into entity, repository, usecase, handler layers.

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

Data types: 0=JSON, 1=Bytes, 2=String.

## Configuration

- `DataDir`: Directory for data files (default: `./data`).
- `MaxFileSize`: Max size per segment in bytes (default: 10MB).

## Testing

Run tests:
```bash
go test ./...
```

Run benchmarks:
```bash
go test -bench=. ./internal/repository/
```

Test data is stored in the `test-data/` directory in the project root for easy inspection and maintenance. Unit tests clean up previous data at the start but leave files after completion for sanity checks. Segmentation tests create multiple segments and dump binary files to human-readable `.txt` versions. End-to-end tests include consistency checks and repair mechanisms.

## Future Enhancements

- Multi-node support.
- Additional connectors (Kafka, Flink).
- Compression, encryption.
- Query capabilities.