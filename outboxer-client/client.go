package client

import (
	"context"
	"time"

	"github.com/BenefexLtd/infrastructure/messaging"
	"io.benefexapps/outboxer/outboxer"
)

// Outbox client - create new outbox records
type OutboxClient struct {
	outboxRepository   outboxer.OutboxRepo
	sequenceRepository outboxer.SequenceRepo
	metrics            outboxer.Metrics
}

// create a new OutboxClient
func NewOutboxClient(outboxRepository outboxer.OutboxRepo, sequenceRepository outboxer.SequenceRepo, metrics outboxer.Metrics) *OutboxClient {
	return &OutboxClient{
		outboxRepository:   outboxRepository,
		sequenceRepository: sequenceRepository,
		metrics:            metrics}
}

// add a new Outbox record
func (oc *OutboxClient) AddOutboxMessage(ctx context.Context, companyId, aggregateType, aggregateID, topic, messageType, createdByUserID string, event messaging.OneHubEvent, eventDate time.Time) (outboxer.Outbox, error) {

	headers := map[string]string{
		"message_type": messageType,
	}

	seq, err := oc.sequenceRepository.GetNextID(ctx, "outbox")
	if err != nil {
		return outboxer.Outbox{}, err
	}

	outbox := outboxer.Outbox{
		ID:              seq.Seq,
		CompanyID:       companyId,
		AggregateType:   aggregateType,
		AggregateID:     aggregateID,
		Status:          outboxer.Created,
		MessageType:     messageType,
		Topic:           topic,
		Headers:         headers,
		Payload:         event,
		EventDate:       eventDate,
		CreatedDate:     time.Now(),
		CreatedByUserID: createdByUserID,
		Retries:         0,
	}

	if _, e := oc.outboxRepository.Add(ctx, outbox); e != nil {
		oc.incrementCreationMetric(outboxer.MetricsError, outbox)
		return outboxer.Outbox{}, err
	}

	oc.incrementCreationMetric(outboxer.MetricsSuccess, outbox)
	return outbox, nil
}

func (oc *OutboxClient) incrementCreationMetric(status string, o outboxer.Outbox) {
	oc.metrics.IncOutboxMessageCreation(status, o.Topic, o.MessageType, o.CompanyID, o.ID)
}
