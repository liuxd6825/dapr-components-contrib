package repository_impl

import (
	"context"
	"fmt"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/repository"
	"gorm.io/gorm"
)

// messageRepository
// @Description: 消息仓储类，所于解决事件补偿处理； 所有Tenant数据存储在一个system表中。
type messageRepository struct {
	dao *dao[*model.Message]
}

const (
	MessageDbId = "system"
)

var msgOptions = NewOptions().SetDbId(MessageDbId)

func NewMessageRepository(db *gorm.DB) repository.MessageRepository {
	_ = db.AutoMigrate(&model.Message{})
	res := &messageRepository{
		dao: NewDao[*model.Message](db,
			func() *model.Message { return &model.Message{} },
			func() []*model.Message { return []*model.Message{} },
		),
	}
	return res
}

func (m *messageRepository) Create(ctx context.Context, v *model.Message) error {
	return m.dao.Insert(ctx, v, msgOptions)
}

func (m *messageRepository) DeleteById(ctx context.Context, tenantId string, id string) error {
	return m.dao.DeleteById(ctx, tenantId, id, msgOptions)
}

func (m *messageRepository) DeleteByAggregateId(ctx context.Context, tenantId, aggregateId string) error {
	filter := fmt.Sprintf(`tenant_id="%v" and aggregate_id="%v"`, tenantId, aggregateId)
	return m.dao.deleteByFilter(ctx, tenantId, filter)
}

func (m *messageRepository) DeleteByAggregateType(ctx context.Context, tenantId, aggregateType string) error {
	filter := fmt.Sprintf(`tenant_id="%v" and aggregate_type="%v"`, tenantId, aggregateType)
	return m.dao.deleteByFilter(ctx, tenantId, filter)
}

func (m *messageRepository) Update(ctx context.Context, v *model.Message) error {
	return m.dao.Update(ctx, v, msgOptions)
}

func (m *messageRepository) FindById(ctx context.Context, tenantId string, id string) (*model.Message, bool, error) {
	return m.dao.FindById(ctx, tenantId, id, msgOptions)
}

func (m *messageRepository) FindAll(ctx context.Context, limit *int64) ([]*model.Message, bool, error) {
	sort := "create_time asc"
	options := NewOptions().SetDbId(MessageDbId).SetSort(&sort)
	return m.dao.findList(ctx, msgOptions.DbId, "", limit, options)
}
