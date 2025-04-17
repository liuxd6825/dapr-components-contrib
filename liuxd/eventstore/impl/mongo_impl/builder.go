package mongo_impl

import (
	"github.com/dapr/components-contrib/liuxd/common"
	"github.com/dapr/components-contrib/liuxd/eventstore"
	db2 "github.com/dapr/components-contrib/liuxd/eventstore/impl/mongo_impl/db"
	repository_impl2 "github.com/dapr/components-contrib/liuxd/eventstore/impl/mongo_impl/repository_impl"
	"github.com/dapr/kit/logger"
)

const (
	ComponentSpecMongo = "eventstore.mongodb"
)

func NewMongoOptions(componentName string, log logger.Logger, metadata common.Metadata, adapter eventstore.GetPubsubAdapter) (*eventstore.Options, error) {
	mongoConfig := db2.NewMongoDB(componentName, log)
	if err := mongoConfig.Init(metadata); err != nil {
		return nil, err
	}

	config := mongoConfig.StorageMetadata()
	event := repository_impl2.NewEventRepository(mongoConfig, config.EventCollectionName())
	snapshot := repository_impl2.NewSnapshotRepository(mongoConfig, config.SnapshotCollectionName())
	aggregate := repository_impl2.NewAggregateRepository(mongoConfig, config.AggregateCollectionName())
	relation := repository_impl2.NewRelationRepository(mongoConfig, config.RelationCollectionName())
	message := repository_impl2.NewMessageRepository(mongoConfig, config.MessageCollectionName())
	session := db2.NewSession(mongoConfig.GetClient())

	ops := &eventstore.Options{
		Metadata:       metadata,
		PubsubAdapter:  adapter,
		EventRepos:     event,
		SnapshotRepos:  snapshot,
		AggregateRepos: aggregate,
		RelationRepos:  relation,
		MessageRepos:   message,
		SnapshotCount:  config.SnapshotCount(),
		Session:        session,
	}

	return ops, nil
}
