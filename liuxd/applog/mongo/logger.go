package mongo

import (
	"context"
	"errors"
	"fmt"
	"github.com/dapr/components-contrib/liuxd/applog"
	"github.com/dapr/components-contrib/liuxd/common"
	pubsub_adapter "github.com/dapr/dapr/pkg/runtime/pubsub"
	"github.com/dapr/kit/logger"
)

type Logger struct {
	componentName   string
	eventLogService EventLogService
	appLogService   AppLogService
	pubsubAdapter   pubsub_adapter.Adapter
	metadata        common.Metadata
	mongodb         *MongoDB
	log             logger.Logger
}

func New(logger logger.Logger) applog.Logger {
	return NewLogger("applog-mongodb", logger)
}

func NewLogger(componentName string, log logger.Logger) applog.Logger {
	return &Logger{
		componentName: componentName,
		log:           log,
	}
}

func (l *Logger) Init(ctx context.Context, metadata common.Metadata, getPubsubAdapter applog.GetPubsubAdapter) (reserr error) {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				reserr = err
			} else {
				reserr = errors.New(fmt.Sprintf("%v", e))
			}

		}
	}()

	l.pubsubAdapter = getPubsubAdapter()
	l.mongodb = NewMongoDB(l.componentName, l.log)
	l.metadata = metadata
	if err := l.mongodb.Init(metadata); err != nil {
		return err
	}

	mongoClient := l.mongodb.GetClient()

	appLogCollection := l.mongodb.NewCollection(l.mongodb.loggerMetadata.appLogCollectionName)
	eventLogCollection := l.mongodb.NewCollection(l.mongodb.loggerMetadata.eventLogCollectionName)

	l.appLogService = NewAppLogService(mongoClient, appLogCollection)
	l.eventLogService = NewEventLogService(mongoClient, eventLogCollection)
	return nil
}

func (l *Logger) WriteAppLog(ctx context.Context, req *applog.WriteAppLogRequest) (resResp *applog.WriteAppLogResponse, reserr error) {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				reserr = err
			} else {
				reserr = errors.New(fmt.Sprintf("%v", e))
			}
		}
	}()

	log := &AppLog{
		Id:       req.Id,
		TenantId: req.TenantId,
		AppId:    req.AppId,
		Class:    req.Class,
		Func:     req.Func,
		Time:     req.Time,
		Level:    req.Level,
		Status:   req.Status,
		Message:  req.Message,
	}
	err := l.appLogService.Insert(ctx, log)
	if err != nil {
		return nil, err
	}
	return &applog.WriteAppLogResponse{}, nil
}

func (l *Logger) UpdateAppLog(ctx context.Context, req *applog.UpdateAppLogRequest) (resResp *applog.UpdateAppLogResponse, reserr error) {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				reserr = err
			} else {
				reserr = errors.New(fmt.Sprintf("%v", e))
			}
		}
	}()

	log := &AppLog{
		Id:       req.Id,
		TenantId: req.TenantId,
		AppId:    req.AppId,
		Class:    req.Class,
		Func:     req.Func,
		Time:     req.Time,
		Level:    req.Level,
		Status:   req.Status,
		Message:  req.Message,
	}
	err := l.appLogService.Update(ctx, log)
	if err != nil {
		return nil, err
	}
	return &applog.UpdateAppLogResponse{}, nil
}

func (l *Logger) GetAppLogById(ctx context.Context, req *applog.GetAppLogByIdRequest) (resResp *applog.GetAppLogByIdResponse, reserr error) {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				reserr = err
			} else {
				reserr = errors.New(fmt.Sprintf("%v", e))
			}
		}
	}()

	log, err := l.appLogService.FindById(ctx, req.TenantId, req.Id)
	if err != nil {
		return nil, err
	}
	if log == nil {
		return nil, nil
	}

	return &applog.GetAppLogByIdResponse{
		Id:       log.Id,
		TenantId: log.TenantId,
		AppId:    log.AppId,
		Class:    log.Class,
		Func:     log.Func,
		Time:     log.Time,
		Level:    log.Level,
		Status:   log.Status,
		Message:  log.Message,
	}, nil
}

func (l *Logger) WriteEventLog(ctx context.Context, req *applog.WriteEventLogRequest) (resResp *applog.WriteEventLogResponse, reserr error) {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				reserr = err
			} else {
				reserr = errors.New(fmt.Sprintf("%v", e))
			}
		}
	}()

	log := &EventLog{
		Id:       req.Id,
		TenantId: req.TenantId,
		AppId:    req.AppId,
		Class:    req.Class,
		Func:     req.Func,
		Time:     req.Time,
		Level:    req.Level,
		Status:   req.Status,
		Message:  req.Message,

		PubAppId:  req.PubAppId,
		EventId:   req.EventId,
		CommandId: req.CommandId,
	}
	err := l.eventLogService.Insert(ctx, log)
	if err != nil {
		return nil, err
	}
	return &applog.WriteEventLogResponse{}, nil
}

func (l *Logger) UpdateEventLog(ctx context.Context, req *applog.UpdateEventLogRequest) (resResp *applog.UpdateEventLogResponse, reserr error) {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				reserr = err
			} else {
				reserr = errors.New(fmt.Sprintf("%v", e))
			}
		}
	}()

	log := &EventLog{
		Id:       req.Id,
		TenantId: req.TenantId,
		AppId:    req.AppId,
		Class:    req.Class,
		Func:     req.Func,
		Time:     req.Time,
		Level:    req.Level,
		Status:   req.Status,
		Message:  req.Message,

		PubAppId:  req.PubAppId,
		EventId:   req.EventId,
		CommandId: req.CommandId,
	}
	err := l.eventLogService.Update(ctx, log)
	if err != nil {
		return nil, err
	}
	return &applog.UpdateEventLogResponse{}, nil
}

func (l *Logger) GetEventLogByCommandId(ctx context.Context, req *applog.GetEventLogByCommandIdRequest) (resResp *applog.GetEventLogByCommandIdResponse, reserr error) {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				reserr = err
			} else {
				reserr = errors.New(fmt.Sprintf("%v", e))
			}
			fmt.Println("***" + reserr.Error())
		}
	}()

	tenantId := req.TenantId
	appId := req.AppId
	commandId := req.CommandId
	list, err := l.eventLogService.FindBySubAppIdAndCommandId(ctx, tenantId, appId, commandId)
	if err != nil {
		return nil, err
	}

	data := make([]applog.EventLogDto, 0)
	if list != nil && len(*list) > 0 {
		for _, log := range *list {
			item := applog.EventLogDto{
				Id:       log.Id,
				TenantId: log.TenantId,
				AppId:    log.AppId,
				Class:    log.Class,
				Func:     log.Func,
				Time:     log.Time,
				Level:    log.Level,
				Status:   log.Status,
				Message:  log.Message,

				PubAppId:  log.PubAppId,
				EventId:   log.EventId,
				CommandId: log.CommandId,
			}
			data = append(data, item)
		}
	}
	resp := &applog.GetEventLogByCommandIdResponse{
		Data: &data,
	}
	return resp, nil
}
