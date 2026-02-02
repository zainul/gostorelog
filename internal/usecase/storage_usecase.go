package usecase

import (
	"gostorelog/internal/entity"
	"gostorelog/internal/repository"
)

// StorageUsecase defines the business logic for storage operations
type StorageUsecase interface {
	StoreRecord(data interface{}, dataType entity.DataType, partitionKey string) error
	RetrieveRecord(partitionKey string, offset uint64) (*entity.Record, error)
}

// storageUsecase implements StorageUsecase
type storageUsecase struct {
	repo repository.StorageRepository
}

// NewStorageUsecase creates a new storage usecase
func NewStorageUsecase(repo repository.StorageRepository) StorageUsecase {
	return &storageUsecase{
		repo: repo,
	}
}

// StoreRecord stores a record
func (u *storageUsecase) StoreRecord(data interface{}, dataType entity.DataType, partitionKey string) error {
	record, err := entity.NewRecord(data, dataType, partitionKey)
	if err != nil {
		return err
	}
	return u.repo.Append(record)
}

// RetrieveRecord retrieves a record by offset
func (u *storageUsecase) RetrieveRecord(partitionKey string, offset uint64) (*entity.Record, error) {
	return u.repo.Read(partitionKey, offset)
}