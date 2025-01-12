package dto

import "github.com/dapr/components-contrib/liuxd/eventstore/domain/model"

type LoadEventRequest struct {
	TenantId      string `json:"tenantId"`
	AggregateId   string `json:"aggregateId"`
	AggregateType string `json:"aggregateType"`
}

type LoadResponse struct {
	TenantId      string                   `json:"tenantId"`
	AggregateId   string                   `json:"aggregateId"`
	AggregateType string                   `json:"aggregateType"`
	Snapshot      *LoadResponseSnapshotDto `json:"snapshot"`
	Events        *[]LoadResponseEventDto  `json:"events"`
	Headers       *ResponseHeaders         `json:"headers"`
}

type LoadResponseSnapshotDto struct {
	AggregateData    map[string]any `json:"aggregateData"`
	AggregateVersion string         `json:"aggregateVersion"`
	SequenceNumber   uint64         `json:"sequenceNumber"`
	Metadata         model.Metadata `json:"metadata"`
}

type LoadResponseEventDto struct {
	EventId        string         `json:"eventId"`
	EventData      map[string]any `json:"eventData"`
	EventType      string         `json:"eventType"`
	EventVersion   string         `json:"eventRevision"`
	SequenceNumber uint64         `json:"sequenceNumber"`
}
