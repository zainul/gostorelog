package usecase

import (
	"gostorelog/internal/entity"
	"gostorelog/internal/repository"
)

// Replicator defines the interface for data replication
type Replicator interface {
	Replicate(data interface{}, dataType entity.DataType, partitionKey string) error
}

// StorageUsecase defines the business logic for storage operations
type StorageUsecase interface {
	StoreRecord(data interface{}, dataType entity.DataType, partitionKey string) error
	RetrieveRecord(partitionKey string, offset uint64) (*entity.Record, error)
	SetReplicator(replicator Replicator)
}

// StorageUsecaseImpl implements StorageUsecase
type StorageUsecaseImpl struct {
	repo       repository.StorageRepository
	Replicator Replicator
}

// NewStorageUsecase creates a new storage usecase
func NewStorageUsecase(repo repository.StorageRepository) StorageUsecase {
	return &StorageUsecaseImpl{
		repo: repo,
	}
}

// SetReplicator sets the replicator for this usecase
func (u *StorageUsecaseImpl) SetReplicator(replicator Replicator) {
	u.Replicator = replicator
}

// StoreRecord stores a record
func (u *StorageUsecaseImpl) StoreRecord(data interface{}, dataType entity.DataType, partitionKey string) error {
	record, err := entity.NewRecord(data, dataType, partitionKey)
	if err != nil {
		return err
	}
	err = u.repo.Append(record)
	if err != nil {
		return err
	}
	// Replicate to followers if replicator is set
	if u.Replicator != nil {
		u.Replicator.Replicate(data, dataType, partitionKey)
	}
	return nil
}

// RetrieveRecord retrieves a record by offset
func (u *StorageUsecaseImpl) RetrieveRecord(partitionKey string, offset uint64) (*entity.Record, error) {
	return u.repo.Read(partitionKey, offset)
}