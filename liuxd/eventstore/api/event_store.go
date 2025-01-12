package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dapr/components-contrib/liuxd/common"
	"github.com/dapr/components-contrib/liuxd/eventstore"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/repository"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/service"
	"github.com/dapr/components-contrib/liuxd/eventstore/dto"
	"github.com/dapr/components-contrib/liuxd/logs"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"strings"
	"time"
)

type EventStore struct {
	log              logger.Logger
	metadata         common.Metadata
	pubsubAdapter    eventstore.GetPubsubAdapter
	eventService     service.EventService
	snapshotService  service.SnapshotService
	aggregateService service.AggregateService
	relationService  service.RelationService
	messageService   service.MessageService
	snapshotCount    uint64
	session          eventstore.Session
}

const DataKey = "dataKey="
const DataKeyLen = len(DataKey)

var JsonContentType = "json"

func NewEventStore(log logger.Logger) eventstore.EventStore {
	return &EventStore{
		log: log,
	}
}

func (s *EventStore) Init(ctx context.Context, opts *eventstore.Options) error {
	if s.log == nil {
		panic("EventStore.log is null")
	}
	s.log.Debugf("Init(opts)")
	s.metadata = opts.Metadata
	s.pubsubAdapter = opts.PubsubAdapter
	s.eventService = service.NewEventService(opts.EventRepos.(repository.EventRepository))
	s.snapshotService = service.NewSnapshotService(opts.SnapshotRepos.(repository.SnapshotRepository))
	s.aggregateService = service.NewAggregateService(opts.AggregateRepos.(repository.AggregateRepository))
	s.relationService = service.NewRelationService(opts.RelationRepos.(repository.RelationRepository))
	s.messageService = service.NewMessageService(opts.MessageRepos.(repository.MessageRepository))
	s.snapshotCount = opts.SnapshotCount
	s.session = opts.Session
	return nil
}

// LoadEvent
// @Description: 加载聚合事件
// @receiver s
// @param ctx
// @param req
// @return *eventstorage.LoadResponse
// @return error
func (s *EventStore) LoadEvent(ctx context.Context, req *dto.LoadEventRequest) (*dto.LoadResponse, error) {
	s.log.Debugf("LoadEvent(ctx,req) req={tenantId:'%v', aggregateId:'%v', aggregateType:'%v'}", req.TenantId, req.AggregateId, req.AggregateType)
	res, err := s.doSession(ctx, req.TenantId, func(ctx context.Context) (interface{}, error) {
		sequenceNumber := uint64(0)
		//获取最后的聚合镜像
		snapshot, ok, err := s.snapshotService.FindByMaxSequenceNumber(ctx, req.TenantId, req.AggregateId, req.AggregateType)
		if err != nil {
			err := newError("findByMaxSequenceNumber() error taking snapshot.", err)
			s.log.Debugf("LoadEvent(ctx,req) return error='%v'", err)
			return nil, err
		}
		if ok {
			sequenceNumber = snapshot.SequenceNumber
		}
		isSourcing := true
		//获取聚合镜像之后的事件列表
		events, ok, err := s.eventService.FindBySequenceNumber(ctx, req.TenantId, req.AggregateId, req.AggregateType, sequenceNumber, &isSourcing)
		if err != nil {
			err := newError("findBySequenceNumber() error taking events.", err)
			s.log.Debugf("LoadEvent(ctx,req) return error='%v'", err)
			return nil, err
		}
		resp := NewLoadResponse(req.TenantId, req.AggregateId, req.AggregateType, snapshot, events)
		s.log.Debugf("LoadEvent(ctx,req) return resp=%v, error='%v'", resp, err)
		return resp, err
	})
	headers := dto.NewResponseHeaders(dto.ResponseStatusSuccess, err, nil)
	if res != nil {
		resp, _ := res.(*dto.LoadResponse)
		resp.Headers = headers
		s.log.Debugf("LoadEvent(ctx,req) return response={} error='%v'", resp, err)
		return resp, err
	}
	resp := &dto.LoadResponse{Headers: headers}
	s.log.Debugf("LoadEvent(ctx,req) return response={} error='%v'", resp, err)
	return resp, err
}

// Commit
// @Description: 提交事务
// @receiver s
// @param ctx
// @param req
// @return *dto.CommitResponse
// @return error
func (s *EventStore) Commit(aCtx context.Context, req *dto.CommitRequest) (*dto.CommitResponse, error) {
	ctx := logs.NewContext(aCtx, s.log)
	s.log.Infof("Commit(ctx,req) start req:{tenantId:'%v', sessionId:'%v'}", req.TenantId, req.SessionId)
	st := time.Now()
	resp := &dto.CommitResponse{}
	if len(req.TenantId) == 0 {
		return resp, errors.New(`error "TenantId" is nil`)
	}
	if len(req.SessionId) == 0 {
		return resp, errors.New(`error "SessionId" is nil`)
	}
	events, isFound, err := s.eventService.FindBySessionIdAndStatus(ctx, req.TenantId, req.SessionId, model.SessionStatusStart)
	if err != nil {
		s.log.Errorf("Commit(ctx,req) eventService.FindBySessionIdAndStatus() error: %s", err.Error())
		return resp, err
	}
	if isFound {
		var data any
		data, err = s.doSession(ctx, req.TenantId, func(ctx context.Context) (interface{}, error) {
			for _, e := range events {
				if err := s.publishEvent(ctx, e); err != nil {
					s.log.Errorf("Commit(ctx,req)  publishEvent() error: %s", err.Error())
					return &dto.CommitResponse{}, err
				}
			}
			if err := s.aggregateService.UpdateSessionStatus(ctx, req.TenantId, req.SessionId, model.SessionStatusNull); err != nil {
				s.log.Errorf("Commit(ctx,req)  aggregateService.UpdateSessionStatus() error: %s ", err.Error())
				return resp, err
			}
			if err := s.eventService.UpdateSessionStatus(ctx, req.TenantId, req.SessionId, model.SessionStatusNull); err != nil {
				s.log.Errorf("Commit(ctx,req) eventService.UpdateSessionStatus() error: %s", err.Error())
				return resp, err
			}
			return resp, nil
		}, IsBatchOption(false))

		resp = data.(*dto.CommitResponse)
		if err != nil {
			return resp, err
		}
	}
	et := time.Now()
	space := et.Sub(st)
	s.log.Infof("Commit(ctx,req) end req:{tenantId:'%v', sessionId:'%v'} useTime: %s,", req.TenantId, req.SessionId, space.String())
	return resp, nil
}

// Rollback
// @Description: 回滚事务
// @receiver s
// @param ctx
// @param req
// @return *dto.RollbackResponse
// @return error
func (s *EventStore) Rollback(ctx context.Context, req *dto.RollbackRequest) (*dto.RollbackResponse, error) {
	s.log.Debugf("Rollback(ctx,req) req={tenantId:'%v', sessionId:'%v'}", req.TenantId, req.SessionId)
	resp := &dto.RollbackResponse{}
	var err error
	if len(req.TenantId) == 0 {
		err = errors.New(`error "TenantId" is nil`)
	}
	if len(req.SessionId) == 0 {
		err = errors.New(`error "SessionId" is nil`)
	}
	if err != nil {
		s.log.Debugf("Rollback(ctx,req) error=%v", err)
	}

	if err = s.aggregateService.DeleteBySessionId(ctx, req.TenantId, req.SessionId); err != nil {
		s.log.Debugf("Rollback(ctx,req) aggregateService.DeleteBySessionId() error=%v", err.Error())
		return resp, err
	}

	if err = s.deleteEventBySessionId(ctx, req.TenantId, req.SessionId); err != nil {
		s.log.Debugf("Rollback(ctx,req) deleteEventBySessionId() error=%v", err.Error())
		return resp, err
	}
	return resp, nil
}

// FindRelations
// @Description: 查找聚合关系列表
// @receiver s
// @param ctx
// @param req
// @return *dto.FindRelationsResponse
// @return error
func (s *EventStore) FindRelations(ctx context.Context, req *dto.FindRelationsRequest) (*dto.FindRelationsResponse, error) {
	res, err := s.doSession(ctx, req.TenantId, func(ctx context.Context) (any, error) {
		findRes, _, err := s.relationService.FindPaging(ctx, req)
		if err != nil {
			return nil, err
		}
		var errMsg string
		if findRes.Error != nil {
			errMsg = findRes.Error.Error()
		}
		var relations []*dto.Relation
		if findRes.Data != nil {
			for _, item := range findRes.Data {
				rel := dto.Relation{
					Id:            item.Id,
					TenantId:      item.TenantId,
					TableName:     item.TableName,
					AggregateId:   item.AggregateId,
					AggregateType: item.AggregateType,
					IsDeleted:     item.IsDeleted,
					RelValue:      item.RelValue,
					RelName:       item.RelName,
				}
				relations = append(relations, &rel)
			}
		}
		res := &dto.FindRelationsResponse{
			Data:       relations,
			TotalRows:  uint64(findRes.TotalRows),
			TotalPages: uint64(findRes.TotalPages),
			PageSize:   uint64(findRes.PageSize),
			PageNum:    uint64(findRes.PageNum),
			Filter:     findRes.Filter,
			Sort:       findRes.Sort,
			Error:      errMsg,
		}
		return res, nil
	})
	headers := dto.NewResponseHeaders(dto.ResponseStatusSuccess, err, nil)
	if res != nil {
		resp, _ := res.(*dto.FindRelationsResponse)
		resp.Headers = headers
		return resp, err
	}
	return &dto.FindRelationsResponse{Headers: headers}, err
}

// FindEvents
// @Description: 查找事件
// @receiver s
// @param ctx
// @param req
// @return *dto.FindEventsResponse
// @return error
func (s *EventStore) FindEvents(ctx context.Context, req *dto.FindEventsRequest) (*dto.FindEventsResponse, error) {
	res := s.eventService.FindPaging(ctx, req)
	var errMessage string
	if res.Error != nil {
		errMessage = res.Error.Error()
	}
	resp := &dto.FindEventsResponse{
		Data:       NewFindEventsItems(res.Data),
		Headers:    dto.NewResponseHeadersSuccess(nil),
		TotalPages: res.TotalPages,
		TotalRows:  res.TotalRows,
		Filter:     res.Filter,
		Sort:       res.Sort,
		Error:      errMessage,
		IsFound:    res.IsFound,
	}
	return resp, res.Error
}

// RepublishMessage
// @Description:  发送消息列表中的事件
// @receiver s
// @param ctx
// @param req
// @return *eventstorage.RepublishMessageResponse
// @return error
func (s *EventStore) RepublishMessage(ctx context.Context, req *dto.RepublishMessageRequest) (*dto.RepublishMessageResponse, error) {
	resp := &dto.RepublishMessageResponse{}
	limit := int64(100)
	if req.Limit > 0 {
		limit = req.Limit
	}
	list, ok, err := s.messageService.FindAll(ctx, &limit)
	if err != nil {
		return nil, err
	}
	if !ok {
		return resp, nil
	}
	for _, item := range list {
		if err := s.publishMessage(ctx, item.Event, true); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

// SaveSnapshot
// @Description: 保存聚合镜像对象
// @receiver s
// @param ctx
// @param req
// @return *eventstorage.SaveSnapshotResponse
// @return error
func (s *EventStore) SaveSnapshot(ctx context.Context, req *dto.SaveSnapshotRequest) (*dto.SaveSnapshotResponse, error) {
	_, err := s.doSession(ctx, req.TenantId, func(ctx context.Context) (interface{}, error) {
		snapshot := &model.Snapshot{
			Id:               model.NewObjectID(),
			TenantId:         req.TenantId,
			AggregateId:      req.AggregateId,
			AggregateType:    req.AggregateType,
			SequenceNumber:   req.SequenceNumber,
			Metadata:         req.Metadata,
			AggregateData:    req.AggregateData,
			AggregateVersion: req.AggregateVersion,
		}

		err := s.snapshotService.Create(ctx, snapshot)
		if err != nil {
			return nil, newError("SnapshotService.Create(). error saving snapshot.", err)
		}
		return nil, nil
	})

	headers := dto.NewResponseHeaders(dto.ResponseStatusSuccess, err, nil)
	return &dto.SaveSnapshotResponse{Headers: headers}, nil
}

// DeleteAggregate
// @Description: 删除聚合根
// @receiver s
// @param ctx
// @param req
// @return *dto.DeleteAggregateResponse
// @return error
func (s *EventStore) DeleteAggregate(ctx context.Context, req *dto.DeleteAggregateRequest) (*dto.DeleteAggregateResponse, error) {
	_, err := s.doSession(ctx, req.TenantId, func(ctx context.Context) (any, error) {
		if req.AggregateId != "" {
			if err := s.aggregateService.DeleteById(ctx, req.TenantId, req.AggregateId); err != nil {
				return nil, err
			}
			if err := s.eventService.DeleteByAggregateId(ctx, req.TenantId, req.AggregateId); err != nil {
				return nil, err
			}
			if err := s.relationService.DeleteByAggregateId(ctx, req.TenantId, req.AggregateId); err != nil {
				return nil, err
			}
			if err := s.messageService.DeleteByAggregateId(ctx, req.TenantId, req.AggregateId); err != nil {
				return nil, err
			}
			if err := s.snapshotService.DeleteByAggregateId(ctx, req.TenantId, req.AggregateId); err != nil {
				return nil, err
			}
		} else {
			if err := s.aggregateService.DeleteByType(ctx, req.TenantId, req.AggregateType); err != nil {
				return nil, err
			}
			if err := s.eventService.DeleteByAggregateType(ctx, req.TenantId, req.AggregateType); err != nil {
				return nil, err
			}
			if err := s.relationService.DeleteByAggregateId(ctx, req.TenantId, req.AggregateType); err != nil {
				return nil, err
			}
			if err := s.messageService.DeleteByAggregateId(ctx, req.TenantId, req.AggregateType); err != nil {
				return nil, err
			}
			if err := s.snapshotService.DeleteByAggregateId(ctx, req.TenantId, req.AggregateType); err != nil {
				return nil, err
			}
		}

		return nil, nil
	})
	return &dto.DeleteAggregateResponse{}, err
}

// ApplyEvent
// @Description:
// @receiver s
// @param ctx
// @param req
// @return *dto.ApplyEventsResponse
// @return error
func (s *EventStore) ApplyEvent(ctx context.Context, req *dto.ApplyEventsRequest) (resp *dto.ApplyEventsResponse, reserr error) {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				reserr = err
			} else {
				reserr = errors.New(fmt.Sprintf("%v", e))
			}
		}
	}()
	isDuplicateEvent := false
	_, err := s.doSession(ctx, req.TenantId, func(ctx context.Context) (any, error) {
		var doErr error
		for i, event := range req.Events {
			switch event.ApplyType {
			case "create":
				doErr = s.doCreateEvent(ctx, req, event)
				break
			case "apply":
				doErr = s.doApplyEvent(ctx, req, event)
				break
			case "delete":
				doErr = s.doDeleteEvent(ctx, req, event)
				break
			default:
				doErr = errors.New(fmt.Sprintf(`req.Events[%v].ApplyType is not ["create", "apply", "delete"]`, i))
				break
			}
			if doErr != nil {
				s.log.Error(doErr)
				return nil, doErr
			}
		}
		return nil, doErr
	}, IsBatchOption(len(req.Events) > 10))

	headers := dto.NewResponseHeaders(dto.ResponseStatusSuccess, err, nil)
	if isDuplicateEvent {
		headers.Status = dto.ResponseStatusEventDuplicate
	}
	resp = &dto.ApplyEventsResponse{}
	resp.Headers = headers
	return resp, err
}

// CreateEvent
// @Description: 创建聚合根并应用领域事件
// @receiver s
// @param ctx
// @param req
// @return *eventstorage.CreateEventResponse
// @return error
func (s *EventStore) doCreateEvent(ctx context.Context, req *dto.ApplyEventsRequest, eventDto *dto.EventDto) error {
	isDuplicateEvent := false
	aggTypeId, err := s.getFieldValue(req.AggregateType, eventDto)
	if err != nil {
		return err
	}
	aggId, err := s.getFieldValue(req.AggregateId, eventDto)
	if err != nil {
		return err
	}

	/*
		agg, ok, err := s.aggregateService.FindById(ctx, req.TenantId, aggTypeId)
		if err != nil {
			return err
		}
		if ok {
			return errors.New(fmt.Sprintf("aggregateId \"%s\" already exists", aggId))
		}*/
	batch, isBatch := model.GetBatch(ctx)
	agg := &model.Aggregate{
		Id:             aggId,
		SessionId:      req.SessionId,
		TenantId:       req.TenantId,
		AggregateId:    aggId,
		AggregateType:  aggTypeId,
		SequenceNumber: 1,
		SessionStatus:  model.SessionStatusNull,
	}
	if len(agg.SessionId) != 0 {
		agg.SessionStatus = model.SessionStatusStart
	}
	if isBatch {
		fmt.Println("start aggId=" + agg.AggregateId)
		batch.AddAggregate(agg)
		fmt.Println("end aggId=" + agg.AggregateId)
	} else if err = s.aggregateService.Create(ctx, agg); err != nil {
		return err
	}

	err = s.saveEvents(ctx, req.TenantId, req.SessionId, aggId, aggTypeId, []*dto.EventDto{eventDto}, agg.SequenceNumber)
	if err, isDuplicateEvent = NotDuplicateKeyError(err); err != nil {
		return err
	}

	headers := dto.NewResponseHeaders(dto.ResponseStatusSuccess, err, nil)
	if isDuplicateEvent {
		headers.Status = dto.ResponseStatusEventDuplicate
	}
	return err
}

// ApplyEvent
// @Description: 应用多个领域事件
// @receiver s
// @param ctx
// @param req
// @return *eventstorage.ApplyEventsResponse
// @return error
func (s *EventStore) doApplyEvent(ctx context.Context, req *dto.ApplyEventsRequest, eventDto *dto.EventDto) error {
	aggType, err := s.getFieldValue(req.AggregateType, eventDto)
	if err != nil {
		return err
	}

	aggId, err := s.getFieldValue(req.AggregateId, eventDto)
	if err != nil {
		return err
	}

	agg, ok, sn, err := s.aggregateService.NextSequenceNumber(ctx, req.TenantId, aggId, aggType, 1)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New(fmt.Sprintf("aggregate id \"%s\" not found", req.AggregateId))
	}
	if agg.Deleted {
		return errors.New(fmt.Sprintf("aggregate id \"%s\" is already deleted.", req.AggregateId))
	}
	return s.saveEvents(ctx, req.TenantId, req.SessionId, aggId, aggType, []*dto.EventDto{eventDto}, sn)
}

// DeleteEvent
// @Description: 发布删除状态的事件
// @receiver s
// @param ctx
// @param req
// @return *eventstorage.DeleteEventResponse
// @return error
func (s *EventStore) doDeleteEvent(ctx context.Context, req *dto.ApplyEventsRequest, eventDto *dto.EventDto) error {
	aggType, err := s.getFieldValue(req.AggregateType, eventDto)
	if err != nil {
		return err
	}
	aggId, err := s.getFieldValue(req.AggregateId, eventDto)
	if err != nil {
		return err
	}

	agg, ok, err := s.aggregateService.SetDeleted(ctx, req.TenantId, aggId)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New(fmt.Sprintf("aggregate id \"%s\" is not fond ", aggId))
	}
	if agg.Deleted {
		return errors.New(fmt.Sprintf("aggregate id \"%s\" is deleted", aggId))
	}
	err = s.saveEvents(ctx, req.TenantId, req.SessionId, aggId, aggType, []*dto.EventDto{eventDto}, agg.SequenceNumber+1)
	if err, _ := NotDuplicateKeyError(err); err != nil {
		return err
	}
	return nil
}

func (s *EventStore) saveEvents(ctx context.Context, tenantId string, sessionId string, aggregateId string, aggregateType string, eventDtoList []*dto.EventDto, startSequenceNumber uint64) error {
	if eventDtoList == nil {
		return errors.New("eventDtoList is nil")
	}
	length := len(eventDtoList)
	if length == 0 {
		return errors.New("request.saveEvents size 0 ")
	}
	batch, isBatch := model.GetBatch(ctx)
	for i := 0; i < length; i++ {
		eventDto := eventDtoList[i]
		event := NewEvent(tenantId, sessionId, aggregateId, aggregateType, startSequenceNumber+uint64(i), eventDto)
		relations := model.NewRelations(tenantId, event.EventId, event.EventType, aggregateId, aggregateType, eventDto.Relations)
		if isBatch {
			batch.AddEvents(event)
			batch.AddRelations(relations...)
		} else {
			err := s.saveEvent(ctx, event, relations)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (s *EventStore) saveEvent(ctx context.Context, event *model.Event, relations []*model.Relation) (reserr error) {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				reserr = err
			} else {
				reserr = errors.New(fmt.Sprintf("%v", e))
			}
		}
	}()
	// 创建新事件，并设置PublishStatus为Wait
	if _, err := s.createEvent(ctx, event); err != nil {
		return newError("createEvent() error saving event.", err)
	}

	// 通过领域事件，保存聚合关系
	if err := s.saveRelations(ctx, event.TenantId, relations); err != nil {
		return newError("relationService.Create() error.", err)
	}

	// 是session模式则退出，待调用Commit()方法发送消息。
	if len(event.SessionId) != 0 {
		return nil
	}

	// 发送事件到消息队列，并设置 PublishStatus 为 PublishStatusSuccess
	if err := s.publishMessage(ctx, event, false); err != nil {
		return err
	}

	return nil
}

func (s *EventStore) publishEvent(ctx context.Context, event *model.Event) error {
	// 发送事件到消息队列，并设置 PublishStatus 为 PublishStatusSuccess
	if err := s.publishMessage(ctx, event, false); err != nil {
		return err
	}
	return nil
}

func (s *EventStore) saveRelations(ctx context.Context, tenantId string, relation []*model.Relation) error {
	if len(relation) == 0 {
		return nil
	}
	if err := s.relationService.CreateMany(ctx, tenantId, relation); err != nil {
		return newError("relationService.Create() error: ", err)
	}
	return nil
}

// publishMessage
// @Description: 发送事件到消息队列，并设置PublishStatus为PublishStatusSuccess
// @receiver s
// @param ctx 上下文
// @param req
// @param event
// @return error
func (s *EventStore) publishMessage(ctx context.Context, event *model.Event, isRepublish bool) error {
	if event == nil {
		return errors.New("publishMessage(ctx, event, isRepublish) error: event is nil")
	}
	tenantId := event.TenantId
	messageId := event.EventId

	// 不是重发
	if !isRepublish {
		message := &model.Message{
			Id:          messageId,
			AggregateId: event.AggregateId,
			TenantId:    tenantId,
			EventId:     event.EventId,
			CreateTime:  time.Now(),
			Event:       event,
			PubsubName:  event.PubsubName,
		}
		if err := s.messageService.Create(ctx, message); err != nil {
			return err
		}
	}

	publishData := dto.PublishData{
		TenantId:       tenantId,
		EventId:        event.EventId,
		EventData:      event.EventData,
		EventVersion:   event.EventVersion,
		EventType:      event.EventType,
		SequenceNumber: event.SequenceNumber,
		Metadata:       event.Metadata,
	}
	bytes, err := json.Marshal(publishData)
	if err != nil {
		return err
	}
	pubData := &pubsub.PublishRequest{
		PubsubName:  event.PubsubName,
		Topic:       event.Topic,
		Metadata:    make(map[string]string),
		ContentType: &JsonContentType,
		Data:        bytes,
	}

	if err := s.pubsubAdapter().Publish(ctx, pubData); err != nil {
		logs.Error(ctx, "publishMessage() eventType:%s, eventId:%s, topic:%s, error:%s", event.EventType, event.Id, event.Topic, err.Error())
		return newError("publishMessage(ctx, event, isRepublish) error: failed to publish event.", err)
	} else {
		logs.Infof(ctx, "publishMessage() eventType:%s, eventId:%s, topic:%s", event.EventType, event.Id, event.Topic)
	}

	if err := s.messageService.Delete(ctx, tenantId, messageId); err != nil {
		return err
	}

	return nil
}

// createEvent
// @Description: 创建事件，并设置发送状态为PublishStatusWait
// @receiver s
// @param ctx
// @param req
// @return *Event
// @return error
func (s *EventStore) createEvent(ctx context.Context, event *model.Event) (*model.Event, error) {
	err := s.eventService.Create(ctx, event)
	return event, err
}

func (s *EventStore) findEventById(ctx context.Context, tenantId string, id string) (*model.Event, bool, error) {
	return s.eventService.FindById(ctx, tenantId, id)
}

func (s *EventStore) deleteEventBySessionId(ctx context.Context, tenantId string, sessionId string) error {
	return s.eventService.DeleteEventBySessionId(ctx, tenantId, sessionId)
}

func newError(msgType string, err error) error {
	return errors.New(msgType + err.Error())
}

func (s *EventStore) newAggregateEntity(req *dto.ApplyEventsRequest) (*model.Aggregate, error) {
	return &model.Aggregate{
		Id:             req.AggregateId,
		TenantId:       req.TenantId,
		AggregateId:    req.AggregateId,
		AggregateType:  req.AggregateType,
		SequenceNumber: 1,
	}, nil
}

type SessionOptions struct {
	isBatch *bool
}

func IsBatchOption(v bool) *SessionOptions {
	isBatch := v
	return &SessionOptions{isBatch: &isBatch}
}

func NewSessionOptions(opts ...*SessionOptions) *SessionOptions {
	res := &SessionOptions{}
	for _, o := range opts {
		if o.isBatch != nil {
			res.isBatch = o.isBatch
		}
	}
	return res
}

func (s *EventStore) doSession(ctx context.Context, tenantId string, fun func(ctx context.Context) (any, error), opts ...*SessionOptions) (any, error) {
	var data interface{}
	var err error
	opt := NewSessionOptions(opts...)
	if opt.IsBatch() {
		ctx, _ = model.NewBatchContext(ctx, tenantId)
	}

	doFun := func(newCtx context.Context) error {
		data, err = fun(newCtx)
		if err != nil {
			return err
		}
		if batch, isBatch := model.GetBatch(newCtx); isBatch {
			if err = s.aggregateService.CreateMany(newCtx, tenantId, batch.Aggregates); err != nil {
				return err
			}
			if err = s.eventService.CreateMany(newCtx, tenantId, batch.Events); err != nil {
				return err
			}
			if err = s.relationService.CreateMany(newCtx, tenantId, batch.Relations); err != nil {
				return err
			}
		}
		return err
	}

	// err = s.session.UseTransaction(ctx, doFun)
	err = doFun(ctx)
	if err != nil {
		s.log.Error(err)
	}
	return data, err
}

func (s *EventStore) GetLogger() logger.Logger {
	return s.log
}

func (s *EventStore) getFieldValue(value string, eventDto *dto.EventDto) (string, error) {
	if strings.HasPrefix(value, DataKey) {
		key := value[DataKeyLen:]
		if data, ok := eventDto.EventData["data"]; ok {
			if mapData, ok := data.(map[string]interface{}); ok {
				if val, ok := mapData[key]; ok {
					if str, ok := val.(string); ok {
						return str, nil
					} else {
						return "", errors.New(fmt.Sprintf("%s is not string", value))
					}
				}
			}
		}
		return "", errors.New(fmt.Sprintf("%s error", value))
	}
	return value, nil
}

func (o *SessionOptions) IsBatch() bool {
	if o.isBatch != nil && *o.isBatch {
		return true
	}
	return false
}
