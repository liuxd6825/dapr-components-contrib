package repository

import (
	"context"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
)

type AggregateRepository interface {
	Create(ctx context.Context, v *model.Aggregate) error
	CreateMany(ctx context.Context, tenantId string, list []*model.Aggregate) error
	DeleteById(ctx context.Context, tenantId string, id string) error
	DeleteByAggregateId(ctx context.Context, tenantId, aggregateId string) error
	DeleteByAggregateType(ctx context.Context, tenantId, aggregateType string) error
	DeleteBySessionId(ctx context.Context, tenantId, sessionId string) error
	Update(ctx context.Context, v *model.Aggregate) error
	UpdateSessionStatus(ctx context.Context, tenantId, sessionId string, status model.SessionStatus) error
	UpdateIsDelete(ctx context.Context, tenantId, aggregateId string) (*model.Aggregate, bool, error)
	FindById(ctx context.Context, tenantId string, id string) (*model.Aggregate, bool, error)
	DeleteAndNextSequenceNumber(ctx context.Context, tenantId, aggregateId string, aggregateType string) (*model.Aggregate, bool, error)
	NextSequenceNumber(ctx context.Context, tenantId string, aggregateId string, aggregateType string, count uint64) (*model.Aggregate, bool, uint64, error)
}
