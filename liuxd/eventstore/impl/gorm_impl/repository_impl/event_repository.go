package repository_impl

import (
	"context"
	"fmt"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/repository"
	"github.com/dapr/components-contrib/liuxd/eventstore/dto"
	"gorm.io/gorm"
)

type eventRepository struct {
	dao *dao[*model.Event]
}

func NewEventRepository(db *gorm.DB) repository.EventRepository {
	_ = db.AutoMigrate(&model.Event{})
	return &eventRepository{
		dao: NewDao[*model.Event](db,
			func() *model.Event { return &model.Event{} },
			func() []*model.Event { return []*model.Event{} },
		),
	}
}

func (r *eventRepository) CreateMany(ctx context.Context, tenantId string, list []*model.Event) error {
	return r.dao.InsertMany(ctx, tenantId, list)
}

func (r eventRepository) Create(ctx context.Context, tenantId string, v *model.Event) error {
	return r.dao.Insert(ctx, v)
}

func (r eventRepository) DeleteById(ctx context.Context, tenantId string, id string) error {
	return r.dao.DeleteById(ctx, tenantId, id)
}

func (r *eventRepository) DeleteByAggregateId(ctx context.Context, tenantId, aggregateId string) error {
	where := fmt.Sprintf(`tenant_id="%v" and aggregate_id="%v"`, tenantId, aggregateId)
	return r.dao.deleteByFilter(ctx, tenantId, where)
}

func (r *eventRepository) DeleteByAggregateType(ctx context.Context, tenantId string, aggregateType string) error {
	filter := fmt.Sprintf(`tenant_id="%v" and aggregate_type="%v"`, tenantId, aggregateType)
	return r.dao.deleteByFilter(ctx, tenantId, filter)
}

func (r eventRepository) Update(ctx context.Context, tenantId string, v *model.Event) error {
	return r.dao.Update(ctx, v)
}

func (r eventRepository) UpdateSessionStatus(ctx context.Context, tenantId string, sessionId string, status model.SessionStatus) error {
	where := fmt.Sprintf(`tenant_id="%v" and session_id="%v"`, tenantId, sessionId)
	values := map[string]any{
		"session_status": status,
	}
	return r.dao.UpdateByWhere(ctx, where, values)
}

func (r eventRepository) FindById(ctx context.Context, tenantId string, id string) (*model.Event, bool, error) {
	return r.dao.FindById(ctx, tenantId, id)
}

func (r *eventRepository) FindPaging(ctx context.Context, query dto.FindPagingQuery) *dto.FindPagingResult[*model.Event] {
	return r.dao.FindPaging(ctx, query)
}

func (r *eventRepository) FindByEventId(ctx context.Context, tenantId string, eventId string) (*model.Event, bool, error) {
	filter := fmt.Sprintf(`tenant_id="%v" and event_id="%v"`, tenantId, eventId)
	return r.dao.findOne(ctx, tenantId, filter)
}

func (r *eventRepository) FindByAggregateId(ctx context.Context, tenantId string, aggregateId string, aggregateType string) ([]*model.Event, bool, error) {
	filter := fmt.Sprintf(`tenant_id="%v" and aggregate_id="%v"`, tenantId, aggregateId)
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
func (r *eventRepository) FindByGtSequenceNumber(ctx context.Context, tenantId string, aggregateId string, aggregateType string, sequenceNumber uint64, isSourcing *bool) ([]*model.Event, bool, error) {
	filter := fmt.Sprintf(`aggregate_id="%v" and aggregate_type="%v"`, aggregateId, aggregateType)
	if isSourcing != nil {
		filter = fmt.Sprintf(`aggregate_id="%v" and aggregate_type="%v" and is_sourcing=%v`, aggregateId, aggregateType, *isSourcing)
	}
	sort := fmt.Sprintf("%v asc", SequenceNumberField)
	findOptions := NewOptions().SetSort(&sort)
	return r.dao.findList(ctx, tenantId, filter, nil, findOptions)
}

func (r *eventRepository) DeleteBySessionId(ctx context.Context, tenantId string, sessionId string) error {
	where := fmt.Sprintf(`session_id="%v"`, sessionId)
	return r.dao.deleteByFilter(ctx, tenantId, where)
}

func (r *eventRepository) FindBySessionIdAndStatus(ctx context.Context, tenantId string, sessionId string, status model.SessionStatus) ([]*model.Event, bool, error) {
	where := fmt.Sprintf(`session_id="%v" and session_status=%v`, sessionId, status)
	var limit int64 = 0
	return r.dao.findList(ctx, tenantId, where, &limit)
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
