package outboxer_server

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"io.benefexapps/outboxer/outboxer"
)

var (
	ErrTopicNotFound = outboxer.ConstError("Unknown Topic")
)

type Publisher struct {
	topics map[string]*pubsub.Topic
}

func NewPublisher(ctx context.Context, client *PubSubClient, topicNames []string) (*Publisher, error) {

	var topics = make(map[string]*pubsub.Topic)
	for _, t := range topicNames {
		topic, err := client.GetOrCreateTopic(ctx, t)
		if err != nil {
			return nil, err
		}
		topics[t] = topic
	}
	return &Publisher{
		topics: topics,
	}, nil
}

// Publish message to pubsub
func (p *Publisher) Publish(ctx context.Context, o outboxer.Outbox) (string, error) {

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
	response := t.Publish(ctx, message)
	return response.Get(ctx)
}
