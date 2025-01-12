package repository_impl

import (
	"context"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/repository"
	"github.com/dapr/components-contrib/liuxd/eventstore/impl/mongo_impl/db"
	"go.mongodb.org/mongo-driver/bson"
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

func NewMessageRepository(mongodb *db.MongoDbConfig, collName string) repository.MessageRepository {
	res := &messageRepository{
		dao: NewDao[*model.Message](mongodb, collName),
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
	filter := bson.M{
		TenantIdField:    tenantId,
		AggregateIdField: aggregateId,
	}
	return m.dao.deleteByFilter(ctx, tenantId, filter, msgOptions)
}
func (m *messageRepository) DeleteByAggregateType(ctx context.Context, tenantId, aggregateType string) error {
	filter := bson.M{
		TenantIdField:      tenantId,
		AggregateTypeField: aggregateType,
	}
	return m.dao.deleteByFilter(ctx, tenantId, filter, msgOptions)
}

func (m *messageRepository) Update(ctx context.Context, v *model.Message) error {
	return m.dao.Update(ctx, v, msgOptions)
}

func (m *messageRepository) FindById(ctx context.Context, tenantId string, id string) (*model.Message, bool, error) {
	return m.dao.FindById(ctx, tenantId, id, msgOptions)
}

// FindAll
// @Description: 取所有Tenant租户中的消息，按创建时间升序排列
// @receiver m
// @param ctx
// @param limit
// @return []*model.Message
// @return bool
// @return error
func (m *messageRepository) FindAll(ctx context.Context, limit *int64) ([]*model.Message, bool, error) {
	filter := bson.M{}
	options := NewOptions().SetDbId(msgOptions.DbId).SetSort(bson.D{{"create_time", 0}})
	return m.dao.findList(ctx, msgOptions.DbId, filter, limit, options)
}
