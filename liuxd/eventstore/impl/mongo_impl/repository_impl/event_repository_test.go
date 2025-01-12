package repository_impl

import (
	"github.com/dapr/components-contrib/liuxd/eventstore/domain/model"
	"golang.org/x/net/context"
	"testing"
)

func TestEventRepository_UpdateSessionStatus(t *testing.T) {
	mongodb, err := newTestMongoDb()
	if err != nil {
		t.Error(err)
		return
	}
	repos := NewEventRepository(mongodb, "events")
	ctx := context.Background()
	if err := repos.UpdateSessionStatus(ctx, TEST_TENANT_ID, "", model.SessionStatusNull); err != nil {
		t.Error(err)
		return
	}
	return
}
