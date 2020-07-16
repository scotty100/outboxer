package outboxer

import (
	"cloud.google.com/go/pubsub"
	"context"
	"log"
)

type PubSubClient struct {
	Psclient *pubsub.Client
}

// getClient creates a google-pubsub client
func GetClient(ctx context.Context, projectID string) (*PubSubClient, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Printf("Error when creating google-pubsub client. Err: %v", err)
		return nil, err
	}
	return &PubSubClient{Psclient: client}, nil
}

func (client *PubSubClient) topicExists(ctx context.Context, topicName string) (bool, error) {
	topic := client.Psclient.Topic(topicName)
	return topic.Exists(ctx)
}

// createTopic creates a topic if a topic name does not exist or returns one
// if it is already present
func (client *PubSubClient) GetOrCreateTopic(ctx context.Context, topicName string) (*pubsub.Topic, error) {
	topicExists, err := client.topicExists(ctx, topicName)
	if err != nil {
		log.Printf("Could not check if topic exists. Error: %+v", err)
		return nil, err
	}
	var topic *pubsub.Topic

	if !topicExists {
		topic, err = client.Psclient.CreateTopic(context.Background(), topicName)
		if err != nil {
			log.Printf("Could not create topic. Err: %+v", err)
			return nil, err
		}
	} else {
		topic = client.Psclient.Topic(topicName)
	}

	return topic, nil
}

