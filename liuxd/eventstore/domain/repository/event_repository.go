package repository

import (
	"context"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/dto"
)

type EventRepository interface {
	Create(ctx context.Context, tenantId string, v *model.Event) error
	CreateMany(ctx context.Context, tenantId string, list []*model.Event) error
	DeleteById(ctx context.Context, tenantId string, id string) error
	DeleteByAggregateId(ctx context.Context, tenantId string, aggregateId string) error
	DeleteByAggregateType(ctx context.Context, tenantId string, aggregateType string) error
	DeleteBySessionId(ctx context.Context, tenantId string, sessionId string) error
	Update(ctx context.Context, tenantId string, v *model.Event) error
	UpdateSessionStatus(ctx context.Context, tenantId string, sessionId string, status model.SessionStatus) error
	FindById(ctx context.Context, tenantId string, id string) (*model.Event, bool, error)
	FindPaging(ctx context.Context, query dto.FindPagingQuery) *dto.FindPagingResult[*model.Event]
	FindByAggregateId(ctx context.Context, tenantId string, aggregateId string, aggregateType string) ([]*model.Event, bool, error)
	FindByGtSequenceNumber(ctx context.Context, tenantId string, aggregateId string, aggregateType string, sequenceNumber uint64, isSourcing *bool) ([]*model.Event, bool, error)
	FindBySessionIdAndStatus(ctx context.Context, tenantId string, sessionId string, status model.SessionStatus) ([]*model.Event, bool, error)
}
