package dto

type RollbackRequest struct {
	SessionId string `json:"sessionId"`
	TenantId  string `json:"tenantId"`
}

type RollbackResponse struct {
	Headers *ResponseHeaders `json:"headers"`
}
