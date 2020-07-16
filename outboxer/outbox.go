package outboxer

import (
	"context"
	utlmongo "github.com/BenefexLtd/onehub-go-base/pkg/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// Possible outbox states.
const (
	Created    = 0
	Processing = 1
	Failed     = 2
	Published  = 3
)

type Outbox struct {
	Id                 string    `json:"id"        bson:"_id"`
	AggregateId        string    `json:"aggregateId"        bson:"aggregateId"`
	Version            string    `json:"version"        bson:"version"`
	Topic              string    `json:"topic"        bson:"topic"`
	MessageType        string    `json:"messageType"        bson:"messageType"`
	Payload            string    `json:"payload"        bson:"payload"`
	State              int       `json:"state"        bson:"state"`
	CreatedDateTime    time.Time `json:"createdDateTime"        bson:"createdDateTime"`
	ProcessingDateTime time.Time `json:"processingDateTime"        bson:"processingDateTime"`
	SentDateTime       time.Time `json:"sentDateTime"        bson:"sentDateTime"`
	ExternalMessageId  string    `json:"externalMessageId"        bson:"externalMessageId"`
	WorkerId           string    `json:"workerId"        bson:"workerId"`
}

type OutboxRepo interface {
	// look at how to optimise this....maybe store the last createdDateTime and get records greater than or equal to that?

	FindNextOutboxesForProcessing(ctx context.Context) (*Outbox, error)

	SetMessageState(ctx context.Context, id string, state int, sentDateTime time.Time, externalMessageId string) error

	Add(ctx context.Context, outbox *Outbox) (*Outbox, error)
}

const OutboxCollection = "outbox"

type MongoOutboxRepo struct {
	Store        *utlmongo.Datastore
	QueryMaxTime int
}

// at moment limit to returning one outbox at a time as FindAndModify limited to 1 document
//
func (r *MongoOutboxRepo) FindNextOutboxesForProcessing(ctx context.Context) (*Outbox, error) {

	options := options.FindOneAndUpdate()
	options.SetSort(bson.D{{"CreatedDateTime", 1}})

	update := bson.M{
		"$set": bson.M{"state": Processing, "processingDateTime": time.Now()},
	}

	res := r.Store.Db.Collection(OutboxCollection).FindOneAndUpdate(ctx, bson.M{"state": Created}, update, options)
	if res.Err() != nil {

		return nil, res.Err()
	}

	var outbox Outbox
	err := res.Decode(&outbox)
	if err != nil {
		return nil, err
	}

	return &outbox, nil
}

func (r *MongoOutboxRepo) SetMessageState(ctx context.Context, id string, state int, sentDateTime time.Time, externalMessageId string) error {

	update := bson.M{
		"$set": bson.M{"state": state, "sentDateTime": sentDateTime, "externalMessageId": externalMessageId},
	}

	_, err := r.Store.Db.Collection(OutboxCollection).UpdateOne(ctx, bson.M{"_id": id}, update, nil)

	return err
}

func (r *MongoOutboxRepo) Add(ctx context.Context, outbox *Outbox) (*Outbox, error) {
	_, err := r.Store.Db.Collection(OutboxCollection).InsertOne(ctx, outbox)
	if err != nil {
		return nil, err
	}

	return outbox, nil
}
