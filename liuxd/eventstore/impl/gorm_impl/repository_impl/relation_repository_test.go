package repository_impl

import (
	ctx "context"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"testing"
)

func Test_RelationServiceCreate(t *testing.T) {
	gormDb, err := newGormDB()
	if err != nil {
		t.Error(err)
		return
	}
	service := NewRelationRepository(gormDb)
	relation := &model.Relation{
		Id:          model.NewObjectID(),
		TenantId:    TEST_TENANT_ID,
		TableName:   "test_relation",
		AggregateId: model.NewObjectID(),
		RelName:     "CaseId",
		RelValue:    "caseId",
	}
	err = service.Create(ctx.Background(), TEST_TENANT_ID, relation)
	if err != nil {
		t.Error(err)
	}
}
