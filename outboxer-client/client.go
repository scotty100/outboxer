package outboxer_client

import (
	"context"
	"github.com/BenefexLtd/infrastructure/messaging"
	"io.benefexapps/outboxer/outboxer"
	"time"
)

type OutboxClient struct {
	outboxRepository   outboxer.OutboxRepo
	sequenceRepository outboxer.SequenceRepo
}

func NewOutboxClient(outboxRepository outboxer.OutboxRepo, sequenceRepository outboxer.SequenceRepo) OutboxClient {
	return OutboxClient{
		outboxRepository:   outboxRepository,
		sequenceRepository: sequenceRepository}
}

func (oc *OutboxClient) AddOutboxMessage(ctx context.Context, companyId, aggregateType, aggregateId, topic, messageType, createdByUserId string, event messaging.OneHubEvent, eventDate time.Time) (outboxer.Outbox, error) {

	headers := map[string]string{
		"message_type": messageType,
	}

	seq, err := oc.sequenceRepository.GetNextId(ctx, "outbox")
	if err != nil {
		return outboxer.Outbox{}, err
	}

	outbox := outboxer.Outbox{
		Id:              seq.Seq,
		CompanyId:       companyId,
		AggregateType:   aggregateType,
		AggregateId:     aggregateId,
		Status:          outboxer.Created,
		MessageType:     messageType,
		Topic:           topic,
		Headers:         headers,
		Payload:         event,
		EventDate:       eventDate,
		CreatedDate:     time.Now(),
		CreatedByUserId: createdByUserId,
		Retries:         0,
	}

	if _, e := oc.outboxRepository.Add(ctx, outbox); e != nil {
		return outboxer.Outbox{}, err
	}

	return outbox, nil
}
