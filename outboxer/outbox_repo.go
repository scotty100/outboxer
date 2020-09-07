package outboxer

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"

	utlmongo "github.com/BenefexLtd/onehub-go-base/pkg/mongo"
)

type OutboxRepo interface {

	GetNextOutbox(ctx context.Context, statuses []string) (Outbox, error)

	SetMessageProcessed(ctx context.Context, id int64, status string, sentDateTime time.Time, externalMessageId string) error

	SetMessagePublishFailed(ctx context.Context, id int64, status string, retries int) error

	Add(ctx context.Context, outbox Outbox) (Outbox, error)
}

const OutboxCollection = "outbox"

type MongoOutboxRepo struct {
	Store        *utlmongo.Datastore
	QueryMaxTime int
}

// at moment limit to returning one outbox at a time as FindAndModify limited to 1 document
//
func (r *MongoOutboxRepo) GetNextOutbox(ctx context.Context, statuses []string) (Outbox, error) {

	queryMaxtime := time.Duration(r.QueryMaxTime) * time.Second
	options := &options.FindOneOptions{
		MaxTime: &queryMaxtime,
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

func (r *MongoOutboxRepo) SetMessageProcessed(ctx context.Context, id string, status int, publishedDateTime time.Time, externalMessageId string) error {

	update := bson.M{
		"$set": bson.M{
			"status": status,
			"publishedDate": publishedDateTime,
			"messageId": externalMessageId},
	}

	_, err := r.Store.Db.Collection(OutboxCollection).UpdateOne(ctx, bson.M{"_id": id}, update, nil)

	return err
}

func (r *MongoOutboxRepo) SetMessagePublishFailed(ctx context.Context, id string, status string, retries int) error {

	update := bson.M{
		"$set": bson.M{
			"status": status,
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
