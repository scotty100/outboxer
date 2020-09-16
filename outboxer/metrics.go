package outboxer

// MetricsError ... constant error status
const MetricsError = "error"

// MetricsSuccess ... constant success status
const MetricsSuccess = "success"

type Metrics interface {
	IncOutboxMessageCreation(status, topic, messageType, companyId string, outboxId int64)
	IncOutboxFind(status string)
	IncOutboxMessagePublish(status, topic, messageType, companyId string, outboxId int64)
	OutboxPublishDelayMillis(milliseconds int64, topic, messageType, companyId string, outboxId int64)
}