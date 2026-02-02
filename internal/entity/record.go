package entity

import (
	"encoding/json"
	"errors"
)

// DataType represents the type of data stored in a record
type DataType int

const (
	DataTypeJSON DataType = iota
	DataTypeBytes
	DataTypeString
)

// Record represents a single entry in the storage engine
type Record struct {
	Offset       uint64   `json:"offset"`       // Sequential offset/index
	Data         []byte   `json:"data"`         // Raw data bytes
	DataType     DataType `json:"data_type"`    // Type of data
	PartitionKey string   `json:"partition_key"` // Key for partitioning
}

// NewRecord creates a new record with the given data and partition key
func NewRecord(data interface{}, dataType DataType, partitionKey string) (*Record, error) {
	var rawData []byte
	var err error

	switch dataType {
	case DataTypeJSON:
		rawData, err = json.Marshal(data)
		if err != nil {
			return nil, err
		}
		// Validate JSON
		var temp interface{}
		if err := json.Unmarshal(rawData, &temp); err != nil {
			return nil, errors.New("invalid JSON data")
		}
	case DataTypeBytes:
		if bytesData, ok := data.([]byte); ok {
			rawData = bytesData
		} else {
			return nil, errors.New("data must be []byte for DataTypeBytes")
		}
	case DataTypeString:
		if strData, ok := data.(string); ok {
			rawData = []byte(strData)
		} else {
			return nil, errors.New("data must be string for DataTypeString")
		}
	default:
		return nil, errors.New("unsupported data type")
	}

	return &Record{
		Data:         rawData,
		DataType:     dataType,
		PartitionKey: partitionKey,
	}, nil
}

// GetData returns the data in its original form
func (r *Record) GetData() (interface{}, error) {
	switch r.DataType {
	case DataTypeJSON:
		var data interface{}
		if err := json.Unmarshal(r.Data, &data); err != nil {
			return nil, err
		}
		return data, nil
	case DataTypeBytes:
		return r.Data, nil
	case DataTypeString:
		return string(r.Data), nil
	default:
		return nil, errors.New("unsupported data type")
	}
}