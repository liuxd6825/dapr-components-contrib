package repository_impl

import (
	"github.com/dapr/components-contrib/liuxd/common"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/impl/mongo_impl/db"
	"github.com/google/uuid"
	"golang.org/x/net/context"
	"testing"
)

const (
	TEST_TENANT_ID = "test"
)

func TestAggregateService_Create(t *testing.T) {
	mongodb, err := newTestMongoDb()
	if err != nil {
		t.Error(err)
		return
	}

	repos := NewAggregateRepository(mongodb, "aggregate")
	agg := &model.Aggregate{
		Id:             newId(),
		TenantId:       TEST_TENANT_ID,
		AggregateId:    newId(),
		AggregateType:  "type",
		SequenceNumber: 1,
	}
	err = repos.Create(context.Background(), agg)
	if err != nil {
		t.Error(err)
	}
}

func TestAggregateRepository_UpdateSessionStatus(t *testing.T) {
	mongodb, err := newTestMongoDb()
	if err != nil {
		t.Error(err)
		return
	}
	repos := NewAggregateRepository(mongodb, "aggregate")
	ctx := context.Background()
	if err := repos.UpdateSessionStatus(ctx, TEST_TENANT_ID, "", model.SessionStatusNull); err != nil {
		t.Error(err)
		return
	}
	return
}

func newId() string {
	return uuid.New().String()
}

func newTestMongoDb() (*db.MongoDbConfig, error) {
	metadata := common.Metadata{
		Properties: map[string]string{
			"host":         "192.168.64.8:27018,192.168.64.8:27019,192.168.64.8:27020",
			"username":     "query-example",
			"password":     "123456",
			"databaseName": "query-example",
		},
	}
	mongodb := db.NewMongoDB(nil)
	if err := mongodb.Init(metadata); err != nil {
		return nil, err
	}
	return mongodb, nil
}
