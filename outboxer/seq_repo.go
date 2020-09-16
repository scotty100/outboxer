package outboxer

import (
	"context"
	"time"

	utlmongo "github.com/BenefexLtd/onehub-go-base/pkg/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Sequence repository
type SequenceRepo interface {
	GetNextID(ctx context.Context, id string) (Sequence, error)
}

// sequence mongo collection name
const SequenceCollection = "sequence"

// Mongo sequence repository implementation
type MongoSeqRepo struct {
	Store        *utlmongo.Datastore
	QueryMaxTime int
}

// Get next sequence id for a provided app id
func (r *MongoSeqRepo) GetNextID(ctx context.Context, id string) (Sequence, error) {
	queryMaxTime := time.Duration(r.QueryMaxTime) * time.Second
	after := options.After
	options := &options.FindOneAndUpdateOptions{
		MaxTime:        &queryMaxTime,
		ReturnDocument: &after,
	}

	update := bson.M{
		"$inc": bson.M{"seq": 1},
	}

	res := r.Store.Db.Collection(SequenceCollection).FindOneAndUpdate(ctx, bson.M{"_id": id}, update, options)
	if res.Err() != nil {

		return Sequence{}, res.Err()
	}

	var seq Sequence
	err := res.Decode(&seq)
	if err != nil {
		return Sequence{}, err
	}

	return seq, nil
}
