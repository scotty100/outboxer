package outboxer

import "time"

// Possible outbox states.
const (
	Created     = 0
	Publishing  = 1
	Published   = 2
	Error_Retry = 3
	Error       = 4
)

type Outbox struct {
	Id              int64             `json:"id"        bson:"_id"`
	CompanyId       string            `json:"companyId"        bson:"companyId"`
	AggregateType   string            `json:"aggregateType"        bson:"aggregateType"`
	AggregateId     string            `json:"aggregateId"        bson:"aggregateId"`
	Status          int               `json:"status"        bson:"status"`
	MessageType     string            `json:"messageType"        bson:"messageType"`
	Topic           string            `json:"topic"        bson:"topic"`
	Headers         map[string]string `json:"headers"        bson:"headers"`
	Payload         interface{}       `json:"payload"        bson:"payload"`
	EventDate       time.Time         `json:"eventDate"        bson:"eventDate"`
	CreatedDate     time.Time         `json:"createdDate"        bson:"createdDate"`
	CreatedByUserId string            `json:"createdByUserId"        bson:"createdByUserId"`
	PublishedDate   *time.Time         `json:"publishedDate"        bson:"publishedDate"`
	MessageId       *string            `json:"messageId"        bson:"messageId"`
	Retries         int               `json:"retries"        bson:"retries"`
}
