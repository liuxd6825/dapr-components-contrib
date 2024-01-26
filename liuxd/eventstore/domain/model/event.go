package model

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Event struct {
	Id             string                 `bson:"_id" json:"id"  gorm:"primaryKey"`
	SessionId      string                 `bson:"session_id" json:"session_id"`
	SessionStatus  SessionStatus          `bson:"session_status" json:"session_status"`
	ApplyType      string                 `bson:"apply_type" json:"apply_type"`
	TenantId       string                 `bson:"tenant_id" json:"tenant_id"`
	CommandId      string                 `bson:"command_id" json:"command_id"`
	EventId        string                 `bson:"event_id" json:"event_id"`
	Metadata       Metadata               `bson:"metadata" json:"metadata" gorm:"type:text;serializer:json"`
	EventData      map[string]interface{} `bson:"event_data" json:"event_data" gorm:"type:text;serializer:json"`
	EventType      string                 `bson:"event_type" json:"event_type"`
	EventVersion   string                 `bson:"event_version" json:"event_version"`
	AggregateId    string                 `bson:"aggregate_id" json:"aggregate_id"`
	AggregateType  string                 `bson:"aggregate_type" json:"aggregate_type"`
	SequenceNumber uint64                 `bson:"sequence_number" json:"sequence_number"`
	TimeStamp      primitive.DateTime     `bson:"time_stamp" json:"time_stamp"`
	Topic          string                 `bson:"topic" json:"topic"`             // 消息主题
	PubsubName     string                 `bson:"pubsub_name" json:"pubsub_name"` // Dapr发布与订阅
	IsSourcing     bool                   `bson:"is_sourcing" json:"is_sourcing"` // 是否溯源
}

type SessionStatus int

const (
	SessionStatusStart SessionStatus = iota
	SessionStatusNull
)

func (s SessionStatus) ToString() string {
	switch s {
	case SessionStatusStart:
		return "doing"
	case SessionStatusNull:
		return ""
	}
	return ""
}

func (a *Event) GetId() string {
	return a.Id
}

func (a *Event) SetId(v string) {
	a.Id = v
}

func (a *Event) GetTenantId() string {
	return a.TenantId
}
