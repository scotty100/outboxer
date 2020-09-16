package outboxer

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	utlmongo "github.com/BenefexLtd/onehub-go-base/pkg/mongo"
)

// Outbox repository interface
type OutboxRepo interface {

	// get the next outbox entry with a status matching the specified statuses
	GetNextOutbox(ctx context.Context, statuses []string) (Outbox, error)

	// set an outbox record as published
	SetMessageProcessed(ctx context.Context, id int64, status string, publishedDateTime time.Time, externalMessageID string) error

	// set an outbox record as failed
	SetMessagePublishFailed(ctx context.Context, id int64, status string, retries int) error

	// add a new outbox record
	Add(ctx context.Context, outbox Outbox) (Outbox, error)
}

// outbox mongo collection name
const OutboxCollection = "outbox"

type MongoOutboxRepo struct {
	Store        *utlmongo.Datastore
	QueryMaxTime int
}

// at moment limit to returning one outbox at a time as FindAndModify limited to 1 document
//
func (r *MongoOutboxRepo) GetNextOutbox(ctx context.Context, statuses []string) (Outbox, error) {

	queryMaxTime := time.Duration(r.QueryMaxTime) * time.Second
	options := &options.FindOneAndUpdateOptions{
		MaxTime: &queryMaxTime,
	}
	update := bson.M{
		"$set": bson.M{"state": Publishing},
	}

	res := r.Store.Db.Collection(OutboxCollection).FindOneAndUpdate(ctx, bson.M{"status": bson.M{"$in": statuses}}, update, options)
	if res.Err() != nil {

		return Outbox{}, res.Err()
	}

	var outbox Outbox
	err := res.Decode(&outbox)
	if err != nil {
		return Outbox{}, err
	}

	return outbox, nil
}

func (r *MongoOutboxRepo) SetMessageProcessed(ctx context.Context, id int64, status string, publishedDateTime time.Time, externalMessageID string) error {

	update := bson.M{
		"$set": bson.M{
			"status":        status,
			"publishedDate": publishedDateTime,
			"messageId":     externalMessageID},
	}

	_, err := r.Store.Db.Collection(OutboxCollection).UpdateOne(ctx, bson.M{"_id": id}, update, nil)

	return err
}

func (r *MongoOutboxRepo) SetMessagePublishFailed(ctx context.Context, id int64, status string, retries int) error {

	update := bson.M{
		"$set": bson.M{
			"status":  status,
			"retries": retries},
	}

	_, err := r.Store.Db.Collection(OutboxCollection).UpdateOne(ctx, bson.M{"_id": id}, update, nil)

	return err
}

func (r *MongoOutboxRepo) Add(ctx context.Context, outbox Outbox) (Outbox, error) {
	_, err := r.Store.Db.Collection(OutboxCollection).InsertOne(ctx, outbox)
	if err != nil {
		return outbox, err
	}

	return outbox, nil
}
