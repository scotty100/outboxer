package outboxer

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
)

// PublisherConfig to be provided by the consumer.
type Publisher struct {
	topic *pubsub.Topic
}

// PublisherConfig to be provided by the consumer.
type PublisherConfig struct {
	ProjectID string
	TopicName string
}

func NewPublisher(ctx context.Context, client *PubSubClient, topicName string) (*Publisher, error) {
	topic, err := client.GetOrCreateTopic(ctx, topicName)
	if err != nil {
		return nil, err
	}
	return &Publisher{
		topic: topic,
	}, nil
}

// Publish message to pubsub
func (publisher *Publisher) Publish(ctx context.Context, o OutboxerEvent) (string, error) {

	attributes := make(map[string]string, 1)
	attributes["message_type"] = o.Outbox.MessageType
	data, err := json.Marshal(o.Outbox.Payload)
	if err != nil {
		return ``, err
	}
	message := &pubsub.Message{
		Data:       data,
		Attributes: attributes,
	}
	response := publisher.topic.Publish(ctx, message)
	return response.Get(ctx)
}
