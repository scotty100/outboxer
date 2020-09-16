package outboxer

import "time"

// Possible outbox states.
const (
	Created     = "CREATED"
	Publishing  = "PUBLISHING"
	Published   = "PUBLISHED"
	Error_Retry = "ERROR_RETRY"
	Error       = "ERROR"
)

type Outbox struct {
	ID              int64             `json:"id"        bson:"_id"`
	CompanyID       string            `json:"companyId"        bson:"companyId"`
	AggregateType   string            `json:"aggregateType"        bson:"aggregateType"`
	AggregateID     string            `json:"aggregateId"        bson:"aggregateId"`
	Status          string            `json:"status"        bson:"status"`
	MessageType     string            `json:"messageType"        bson:"messageType"`
	Topic           string            `json:"topic"        bson:"topic"`
	Headers         map[string]string `json:"headers"        bson:"headers"`
	Payload         interface{}       `json:"payload"        bson:"payload"`
	EventDate       time.Time         `json:"eventDate"        bson:"eventDate"`
	CreatedDate     time.Time         `json:"createdDate"        bson:"createdDate"`
	CreatedByUserID string            `json:"createdByUserId"        bson:"createdByUserId"`
	PublishedDate   *time.Time        `json:"publishedDate"        bson:"publishedDate"`
	MessageID       *string           `json:"messageId"        bson:"messageId"`
	Retries         int               `json:"retries"        bson:"retries"`
}
