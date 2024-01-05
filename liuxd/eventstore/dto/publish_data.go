package dto

type PublishData struct {
	TenantId       string      `json:"tenantId"`
	EventId        string      `json:"eventId"`
	EventData      interface{} `json:"eventData"`
	EventType      string      `json:"eventType"`
	EventVersion   string      `json:"eventVersion"`
	SequenceNumber uint64      `json:"sequenceNumber"`
}
