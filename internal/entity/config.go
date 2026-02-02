package entity

// Config holds configuration for the storage engine
type Config struct {
	DataDir     string `json:"data_dir"`     // Directory to store data files
	MaxFileSize uint64 `json:"max_file_size"` // Max size of a segment file in bytes (e.g., 10MB)
}