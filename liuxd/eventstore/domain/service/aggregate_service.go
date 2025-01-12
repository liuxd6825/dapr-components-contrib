package service

import (
	"context"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/repository"
)

type AggregateService interface {
	Create(ctx context.Context, req *model.Aggregate) error
	CreateMany(ctx context.Context, tenantId string, list []*model.Aggregate) error
	UpdateSessionStatus(ctx context.Context, tenantId, sessionId string, status model.SessionStatus) error
	DeleteById(ctx context.Context, tenantId, aggregateId string) error
	DeleteByType(ctx context.Context, tenantId, aggregateType string) error
	DeleteBySessionId(ctx context.Context, tenantId, sessionId string) error
	SetDeleted(ctx context.Context, tenantId, aggregateId string) (*model.Aggregate, bool, error)
	DeleteAndNextSequenceNumber(ctx context.Context, tenantId, aggregateId, aggregateType string) (*model.Aggregate, bool, error)
	FindById(ctx context.Context, tenantId, aggregateId string) (*model.Aggregate, bool, error)
	NextSequenceNumber(ctx context.Context, tenantId, aggregateId, aggregateType string, count uint64) (*model.Aggregate, bool, uint64, error)
}

func NewAggregateService(repos repository.AggregateRepository) AggregateService {
	return &aggregateService{repos: repos}
}

type aggregateService struct {
	repos repository.AggregateRepository
}

func (c *aggregateService) CreateMany(ctx context.Context, tenantId string, list []*model.Aggregate) error {
	return c.repos.CreateMany(ctx, tenantId, list)
}

func (c *aggregateService) UpdateSessionStatus(ctx context.Context, tenantId, sessionId string, status model.SessionStatus) error {
	return c.repos.UpdateSessionStatus(ctx, tenantId, sessionId, status)
}

func (c *aggregateService) DeleteBySessionId(ctx context.Context, tenantId, sessionId string) error {
	return c.repos.DeleteBySessionId(ctx, tenantId, sessionId)
}

func (c *aggregateService) DeleteById(ctx context.Context, tenantId, aggregateId string) error {
	return c.repos.DeleteByAggregateId(ctx, tenantId, aggregateId)
}

func (c *aggregateService) DeleteByType(ctx context.Context, tenantId, aggregateType string) error {
	return c.repos.DeleteByAggregateType(ctx, tenantId, aggregateType)
}

func (c *aggregateService) Destroy(ctx context.Context, tenantId, aggregateId string) error {
	return c.repos.DeleteById(ctx, tenantId, aggregateId)
}

func (c *aggregateService) Create(ctx context.Context, agg *model.Aggregate) error {
	if agg == nil {
		return nil
	}
	return c.repos.Create(ctx, agg)
}

func (c *aggregateService) SetDeleted(ctx context.Context, tenantId, aggregateId string) (*model.Aggregate, bool, error) {
	return c.repos.UpdateIsDelete(ctx, tenantId, aggregateId)
}

func (c *aggregateService) DeleteAndNextSequenceNumber(ctx context.Context, tenantId, aggregateId, aggregateType string) (*model.Aggregate, bool, error) {
	return c.repos.DeleteAndNextSequenceNumber(ctx, tenantId, aggregateId, aggregateType)
}

func (c *aggregateService) FindById(ctx context.Context, tenantId, aggregateId string) (*model.Aggregate, bool, error) {
	return c.repos.FindById(ctx, tenantId, aggregateId)
}

func (c *aggregateService) NextSequenceNumber(ctx context.Context, tenantId, aggregateId, aggregateType string, count uint64) (*model.Aggregate, bool, uint64, error) {
	return c.repos.NextSequenceNumber(ctx, tenantId, aggregateId, aggregateType, count)
}
