package service

import (
	"context"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/repository"
)

type MessageService interface {
	Create(ctx context.Context, msg *model.Message) error
	Delete(ctx context.Context, tenantId, id string) error
	DeleteByAggregateId(ctx context.Context, tenantId, aggregateId string) error
	DeleteByAggregateType(ctx context.Context, tenantId, aggregateType string) error
	FindAll(ctx context.Context, limit *int64) ([]*model.Message, bool, error)
}

func NewMessageService(repos repository.MessageRepository) MessageService {
	return &messageService{repos: repos}
}

type messageService struct {
	repos repository.MessageRepository
}

func (m *messageService) Create(ctx context.Context, msg *model.Message) error {
	if msg == nil {
		return nil
	}
	return m.repos.Create(ctx, msg)
}

func (m *messageService) Delete(ctx context.Context, tenantId, id string) error {
	return m.repos.DeleteById(ctx, tenantId, id)
}

func (m *messageService) DeleteByAggregateId(ctx context.Context, tenantId, aggregateId string) error {
	return m.repos.DeleteByAggregateId(ctx, tenantId, aggregateId)
}

func (c *messageService) DeleteByAggregateType(ctx context.Context, tenantId, aggregateType string) error {
	return c.repos.DeleteByAggregateType(ctx, tenantId, aggregateType)
}

func (m *messageService) FindAll(ctx context.Context, limit *int64) ([]*model.Message, bool, error) {
	return m.repos.FindAll(ctx, limit)
}
