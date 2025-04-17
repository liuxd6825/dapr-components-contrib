package gorm_impl

import (
	"github.com/dapr/components-contrib/liuxd/common"
	"github.com/dapr/components-contrib/liuxd/eventstore"
	"github.com/dapr/components-contrib/liuxd/eventstore/impl/gorm_impl/db"
	"github.com/dapr/components-contrib/liuxd/eventstore/impl/gorm_impl/repository_impl"
	"github.com/dapr/kit/logger"
)

const (
	ComponentSpecMySql = "eventstore.mysql"
)

func NewMySqlOptions(componentName string, log logger.Logger, metadata common.Metadata, adapter eventstore.GetPubsubAdapter) (*eventstore.Options, error) {
	mysqlConfig := db.NewMySqlDB(log)
	if err := mysqlConfig.Init(metadata); err != nil {
		return nil, err
	}

	gormDB := mysqlConfig.GetDB()

	config := mysqlConfig.StorageMetadata()
	event := repository_impl.NewEventRepository(gormDB)
	snapshot := repository_impl.NewSnapshotRepository(gormDB)
	aggregate := repository_impl.NewAggregateRepository(gormDB)
	relation := repository_impl.NewRelationRepository(gormDB)
	message := repository_impl.NewMessageRepository(gormDB)
	session := db.NewSession(gormDB)

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
