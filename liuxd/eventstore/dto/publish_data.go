package dto

import "github.com/dapr/components-contrib/liuxd/eventstore/domain/model"

type PublishData struct {
	TenantId       string         `json:"tenantId"`
	EventId        string         `json:"eventId"`
	EventData      interface{}    `json:"eventData"`
	Metadata       model.Metadata `json:"metadata"`
	EventType      string         `json:"eventType"`
	EventVersion   string         `json:"eventVersion"`
	SequenceNumber uint64         `json:"sequenceNumber"`
}
