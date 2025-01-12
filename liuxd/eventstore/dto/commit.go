package dto

type CommitRequest struct {
	SessionId string `json:"sessionId"`
	TenantId  string `json:"tenantId"`
}

type CommitResponse struct {
	Headers *ResponseHeaders `json:"headers"`
}
