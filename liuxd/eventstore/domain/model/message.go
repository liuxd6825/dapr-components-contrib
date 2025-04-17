package model

import (
	"time"
)

type Message struct {
	Id            string    `bson:"_id" json:"id"  gorm:"primaryKey"`
	AggregateId   string    `bson:"aggregate_id" json:"aggregate_id"`
	AggregateType string    `bson:"aggregate_type" json:"aggregate_type"`
	TenantId      string    `bson:"tenant_id" json:"tenant_id"`
	EventId       string    `bson:"event_id" json:"event_id"`
	CreateTime    time.Time `bson:"create_time" json:"create_time"`
	Event         *Event    `bson:"event" json:"event"  gorm:"type:text;serializer:json"`
	PubsubName    string    `bson:"pubsub_name" json:"pubsubName" gorm:"pubsub_name"`
}

func (a *Message) GetId() string {
	return a.Id
}

func (a *Message) SetId(v string) {
	a.Id = v
}

func (a *Message) GetTenantId() string {
	return a.TenantId
}

func (a *Message) GetPubsubName() string {
	return a.PubsubName
}
