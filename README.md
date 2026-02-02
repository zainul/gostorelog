# GoStoreLog

A persistent, sequential, immutable storage engine for Go, designed with clean architecture.

## Features

- **Persistent Storage**: Data is stored on disk in `.store` and `.index` files.
- **Sequential Ordering**: Records are appended in order, with immutable offsets.
- **Data Types**: Supports JSON, bytes, and strings.
- **Partitioning**: Data can be partitioned by user-defined keys.
- **Auto-Segmentation**: Files are segmented when reaching max size (e.g., 10MB).
- **Atomic Appends**: Ensures data safety during writes.
- **Pub/Sub Integration**: Uses a generic connector for message handling (currently Go channels, extensible to Kafka, etc.).
- **Recovery**: Automatically loads existing data on startup.
- **Clean Architecture**: Organized into entity, repository, usecase, handler layers.

## Architecture

- `internal/entity`: Data models and business entities.
- `internal/repository`: Data access layer for storage.
- `internal/usecase`: Business logic.
- `internal/handler`: Pub/Sub handlers and connectors.
- `pkg`: Generic utilities like logging.
- `cmd`: Application entry point.

## Usage

1. Build and run:
   ```bash
   go run cmd/main.go
   ```

2. Publish messages to the topic via the connector.

## Configuration

- `DataDir`: Directory for data files (default: `./data`).
- `MaxFileSize`: Max size per segment in bytes (default: 10MB).

## Testing

Run tests:
```bash
go test ./...
```

## Future Enhancements

- Multi-node support.
- Additional connectors (Kafka, Flink).
- Compression, encryption.
- Query capabilities.