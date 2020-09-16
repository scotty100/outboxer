package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"io.benefexapps/outboxer/outboxer"
)

var (
	// ErrTopicNotFound error when a google pubsub topic is not known
	ErrTopicNotFound = outboxer.ConstError("Unknown Topic")
)

// Publisher struct to publish messages to pubsub
type Publisher struct {
	topics  map[string]*pubsub.Topic
	metrics outboxer.Metrics
}

// NewPublisher crete a new publisher
func NewPublisher(ctx context.Context, client *PubSubClient, topicNames []string, metrics outboxer.Metrics) (Publisher, error) {

	var topics = make(map[string]*pubsub.Topic)
	for _, t := range topicNames {
		topic, err := client.GetOrCreateTopic(ctx, t)
		if err != nil {
			return Publisher{}, err
		}
		topics[t] = topic
	}
	return Publisher{
		topics:  topics,
		metrics: metrics,
	}, nil
}

// Publish message to pubsub
func (p Publisher) Publish(ctx context.Context, o outboxer.Outbox) (string, error) {

	t, ok := p.topics[o.Topic]
	if !ok {
		return "", ErrTopicNotFound.Wrap(fmt.Errorf("%s not known", o.Topic))
	}

	data, err := json.Marshal(o.Payload)
	if err != nil {
		return ``, err
	}
	message := &pubsub.Message{
		Data:       data,
		Attributes: o.Headers,
	}

	res := outboxer.MetricsSuccess
	response := t.Publish(ctx, message)
	messID, err := response.Get(ctx)
	if err != nil {
		res = outboxer.MetricsError
	}
	p.firePublishMetrics(res, o)
	return messID, err
}

func (p Publisher) firePublishMetrics(status string, o outboxer.Outbox) {
	p.metrics.IncOutboxMessagePublish(status, o.Topic, o.MessageType, o.CompanyID, o.ID)
	if status == outboxer.MetricsSuccess {
		p.metrics.OutboxPublishDelayMillis(time.Now().Sub(o.CreatedDate).Milliseconds(), o.Topic, o.MessageType, o.CompanyID, o.ID)
	}
}
