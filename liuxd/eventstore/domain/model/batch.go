package model

import "context"

type batchKey struct {
}

type Batch struct {
	tenantId   string
	Aggregates []*Aggregate
	Events     []*Event
	Relations  []*Relation
}

func (b *Batch) AddAggregate(agg ...*Aggregate) {
	for _, i := range agg {
		b.Aggregates = append(b.Aggregates, i)
	}
}

func (b *Batch) AddEvents(event ...*Event) {
	for _, i := range event {
		b.Events = append(b.Events, i)
	}
}

func (b *Batch) AddRelations(relations ...*Relation) {
	for _, i := range relations {
		b.Relations = append(b.Relations, i)
	}
}

func newBatch(tenantId string) *Batch {
	return &Batch{
		tenantId: tenantId,
	}
}

func NewBatchContext(parent context.Context, tenantId string) (context.Context, *Batch) {
	ctx, _ := context.WithCancel(parent)
	value := newBatch(tenantId)
	return context.WithValue(ctx, batchKey{}, value), value
}

func GetBatch(ctx context.Context) (*Batch, bool) {
	value := ctx.Value(batchKey{})
	if value == nil {
		return nil, false
	}
	events, ok := value.(*Batch)
	return events, ok
}
