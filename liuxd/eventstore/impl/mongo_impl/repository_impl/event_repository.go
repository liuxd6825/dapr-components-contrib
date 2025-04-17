package repository_impl

import (
	"context"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/repository"
	"github.com/dapr/components-contrib/liuxd/eventstore/dto"
	"github.com/dapr/components-contrib/liuxd/eventstore/impl/mongo_impl/db"
	"go.mongodb.org/mongo-driver/bson"
)

type eventRepository struct {
	dao *dao[*model.Event]
}

func (r *eventRepository) CreateMany(ctx context.Context, tenantId string, list []*model.Event) error {
	return r.dao.InsertMany(ctx, tenantId, list)
}

func NewEventRepository(mongodb *db.MongoDbConfig, collName string) repository.EventRepository {
	return &eventRepository{
		dao: NewDao[*model.Event](mongodb, collName),
	}
}

func (r *eventRepository) UpdateSessionStatus(ctx context.Context, tenantId string, sessionId string, status model.SessionStatus) error {
	filter := bson.M{
		TenantIdField:  tenantId,
		SessionIdField: sessionId,
	}
	setData := bson.D{
		{"$set", bson.D{
			{SessionStatusField, status},
		}},
	}
	return r.dao.updateByFilter(ctx, tenantId, filter, setData)
}

func (r *eventRepository) Create(ctx context.Context, tenantId string, v *model.Event) error {
	return r.dao.Insert(ctx, v)
}

func (r *eventRepository) DeleteById(ctx context.Context, tenantId string, id string) error {
	return r.dao.DeleteById(ctx, tenantId, id)
}

func (r *eventRepository) DeleteByAggregateId(ctx context.Context, tenantId, aggregateId string) error {
	filter := bson.M{
		TenantIdField:    tenantId,
		AggregateIdField: aggregateId,
	}
	return r.dao.deleteByFilter(ctx, tenantId, filter)
}

func (r *eventRepository) DeleteByAggregateType(ctx context.Context, tenantId, aggregateType string) error {
	filter := bson.M{
		TenantIdField:      tenantId,
		AggregateTypeField: aggregateType,
	}
	return r.dao.deleteByFilter(ctx, tenantId, filter)
}

func (r eventRepository) Update(ctx context.Context, tenantId string, v *model.Event) error {
	return r.dao.Update(ctx, v)
}

func (r eventRepository) FindById(ctx context.Context, tenantId string, id string) (*model.Event, bool, error) {
	return r.dao.FindById(ctx, tenantId, id)
}

func (r *eventRepository) FindPaging(ctx context.Context, query dto.FindPagingQuery) *dto.FindPagingResult[*model.Event] {
	return r.dao.FindPaging(ctx, query)
}

func (r *eventRepository) FindByEventId(ctx context.Context, tenantId string, eventId string) (*model.Event, bool, error) {
	filter := bson.M{
		TenantIdField: tenantId,
		EventIdField:  eventId,
	}
	return r.dao.findOne(ctx, tenantId, filter)
}

func (r *eventRepository) FindByAggregateId(ctx context.Context, tenantId string, aggregateId string, aggregateType string) ([]*model.Event, bool, error) {
	filter := bson.M{
		TenantIdField:      tenantId,
		AggregateIdField:   aggregateId,
		AggregateTypeField: aggregateType,
	}
	return r.dao.findList(ctx, tenantId, filter, nil)
}

// FindByGtSequenceNumber
// @Description: 查找大于SequenceNumber的事件
// @receiver r
// @param ctx
// @param tenantId
// @param aggregateId
// @param aggregateType
// @param sequenceNumber
// @return []*model.Event
// @return bool
// @return error
func (r *eventRepository) FindByGtSequenceNumber(ctx context.Context, tenantId string, aggregateId string, aggregateType string, sequenceNumber uint64, isSouring *bool) ([]*model.Event, bool, error) {
	filter := bson.M{
		TenantIdField:       tenantId,
		AggregateIdField:    aggregateId,
		AggregateTypeField:  aggregateType,
		SequenceNumberField: bson.M{"$gt": sequenceNumber},
	}
	if isSouring != nil {
		filter = bson.M{
			TenantIdField:       tenantId,
			AggregateIdField:    aggregateId,
			AggregateTypeField:  aggregateType,
			SequenceNumberField: bson.M{"$gt": sequenceNumber},
			IsSourcingField:     *isSouring,
		}
	}

	findOptions := NewOptions().SetSort(bson.D{{SequenceNumberField, 1}})
	return r.dao.findList(ctx, tenantId, filter, nil, findOptions)
}

func (r eventRepository) DeleteBySessionId(ctx context.Context, tenantId string, sessionId string) error {
	filter := bson.M{
		TenantIdField:  tenantId,
		SessionIdField: sessionId,
	}
	return r.dao.deleteByFilter(ctx, tenantId, filter)
}

func (r eventRepository) FindBySessionIdAndStatus(ctx context.Context, tenantId string, sessionId string, status model.SessionStatus) ([]*model.Event, bool, error) {
	filter := bson.M{
		TenantIdField:      tenantId,
		SessionIdField:     sessionId,
		SessionStatusField: status,
	}
	var limit int64 = 0
	return r.dao.findList(ctx, tenantId, filter, &limit)
}

/*
func (r *eventRepository) NextSequenceNumber(ctx context.Context, tenantId string, aggregateId string, aggregateType string) uint64 {
	filter := bson.M{
		TenantIdField:      tenantId,
		AggregateIdField:   aggregateId,
		AggregateTypeField: aggregateType,
	}
	findOptions := options.FindOne().SetSort(bson.D{{SequenceNumberField, -1}})
	result := r.collection.FindOne(ctx, filter, findOptions)
	var event model.Event
	if err := result.Decode(&event); err == nil {
		return event.SequenceNumber + 1
	}
	return 1
}
*/
