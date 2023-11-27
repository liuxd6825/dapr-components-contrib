package dto

type ApplyEventsRequest struct {
	SessionId     string `json:"sessionId"`
	TenantId      string `json:"tenantId"`
	AggregateId   string `json:"aggregateId"`
	AggregateType string `json:"aggregateType"`
	Events        []*EventDto
}

type ApplyEventsResponse struct {
	Headers *ResponseHeaders `json:"headers"`
}
