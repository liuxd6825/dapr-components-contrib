package repository_impl

import (
	"context"
	"fmt"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/repository"
	"github.com/dapr/components-contrib/liuxd/eventstore/dto"
	cmap "github.com/orcaman/concurrent-map"
	"gorm.io/gorm"
)

var collections = cmap.New()

type relationRepository struct {
	dao *dao[*model.Relation]
}

func NewRelationRepository(db *gorm.DB) repository.RelationRepository {
	_ = db.AutoMigrate(&model.Relation{})
	res := &relationRepository{
		dao: NewDao[*model.Relation](db,
			func() *model.Relation { return &model.Relation{} },
			func() []*model.Relation { return []*model.Relation{} },
		),
	}
	return res
}

func (r *relationRepository) Create(ctx context.Context, tenantId string, v *model.Relation) error {
	return r.dao.Insert(ctx, v)
}

func (r *relationRepository) CreateMany(ctx context.Context, tenantId string, vList []*model.Relation) error {
	return r.dao.InsertMany(ctx, tenantId, vList)
}

func (r *relationRepository) DeleteById(ctx context.Context, tenantId string, id string) error {
	return r.dao.DeleteById(ctx, tenantId, id)
}

func (r *relationRepository) DeleteByAggregateId(ctx context.Context, tenantId, aggregateId string) error {
	filter := fmt.Sprintf(`tenant_id="%v" and aggregate_id="%v"`, tenantId, aggregateId)
	return r.dao.deleteByFilter(ctx, tenantId, filter)
}

func (r *relationRepository) DeleteByAggregateType(ctx context.Context, tenantId string, aggregateType string) error {
	filter := fmt.Sprintf(`tenant_id="%v" and aggregate_type="%v"`, tenantId, aggregateType)
	return r.dao.deleteByFilter(ctx, tenantId, filter)
}

func (r *relationRepository) Update(ctx context.Context, tenantId string, v *model.Relation) error {
	return r.dao.Update(ctx, v)
}

func (r *relationRepository) FindById(ctx context.Context, tenantId string, id string) (*model.Relation, bool, error) {
	return r.dao.FindById(ctx, tenantId, id)
}

func (r *relationRepository) FindPaging(ctx context.Context, query dto.FindPagingQuery) (*dto.FindPagingResult[*model.Relation], bool, error) {
	return r.dao.findPaging(ctx, query).Result()
}
