package cli

import (
	"testing"

	"github.com/ulbwa/ipfsgram/internal/model"
)

func ref(carID int64) model.BlockRef {
	return model.BlockRef{CarID: carID, Offset: 100, Length: 42}
}

func TestPlanDedup_PublishedOKSkips(t *testing.T) {
	plan := planDedup(
		map[string]model.BlockRef{"a": ref(1), "b": ref(1)},
		map[int64]model.CarStatus{1: model.CarPublished},
		map[int64]carCheck{1: checkOK},
	)
	if !plan.Skip["a"] || !plan.Skip["b"] {
		t.Errorf("blocks of a live published car must be skipped: %+v", plan)
	}
	if len(plan.Reupload) != 0 || len(plan.DeleteCars) != 0 || len(plan.SetStatus) != 0 {
		t.Errorf("no reupload/delete/status expected: %+v", plan)
	}
}

func TestPlanDedup_MessageDeletedReuploadsAndDeletes(t *testing.T) {
	plan := planDedup(
		map[string]model.BlockRef{"a": ref(1)},
		map[int64]model.CarStatus{1: model.CarPublished},
		map[int64]carCheck{1: checkDeleted},
	)
	if !plan.Reupload["a"] || plan.Skip["a"] {
		t.Errorf("blocks of a deleted car must be re-uploaded: %+v", plan)
	}
	if len(plan.DeleteCars) != 1 || plan.DeleteCars[0] != 1 {
		t.Errorf("deleted car must be scheduled for row deletion: %+v", plan)
	}
	if len(plan.SetStatus) != 0 {
		t.Errorf("deleted car must not get a status marker: %+v", plan)
	}
}

func TestPlanDedup_NoAccessReuploadsAndMarks(t *testing.T) {
	plan := planDedup(
		map[string]model.BlockRef{"a": ref(1)},
		map[int64]model.CarStatus{1: model.CarPublished},
		map[int64]carCheck{1: checkNoAccess},
	)
	if !plan.Reupload["a"] {
		t.Errorf("blocks of an unreachable car must be re-uploaded: %+v", plan)
	}
	if got := plan.SetStatus[1]; got != model.CarNoBotAccess {
		t.Errorf("SetStatus[1] = %q, want no_bot_access", got)
	}
	if len(plan.DeleteCars) != 0 {
		t.Errorf("recoverable car must not be deleted: %+v", plan)
	}
}

func TestPlanDedup_TooLargeReuploadsAndMarks(t *testing.T) {
	plan := planDedup(
		map[string]model.BlockRef{"a": ref(1)},
		map[int64]model.CarStatus{1: model.CarPublished},
		map[int64]carCheck{1: checkTooLarge},
	)
	if !plan.Reupload["a"] {
		t.Errorf("blocks of a too-large car must be re-uploaded: %+v", plan)
	}
	if got := plan.SetStatus[1]; got != model.CarTooLarge {
		t.Errorf("SetStatus[1] = %q, want too_large", got)
	}
}

func TestPlanDedup_AlreadyMarkedCarReuploadsWithoutChanges(t *testing.T) {
	plan := planDedup(
		map[string]model.BlockRef{"a": ref(1)},
		map[int64]model.CarStatus{1: model.CarNoBotAccess},
		map[int64]carCheck{},
	)
	if !plan.Reupload["a"] {
		t.Errorf("blocks of a no_bot_access car must be re-uploaded: %+v", plan)
	}
	if len(plan.SetStatus) != 0 || len(plan.DeleteCars) != 0 {
		t.Errorf("already-marked car needs no changes: %+v", plan)
	}
}

func TestPlanDedup_MixedCars(t *testing.T) {
	plan := planDedup(
		map[string]model.BlockRef{"live": ref(1), "dead": ref(2), "lost": ref(3)},
		map[int64]model.CarStatus{
			1: model.CarPublished,
			2: model.CarPublished,
			3: model.CarPublished,
		},
		map[int64]carCheck{1: checkOK, 2: checkDeleted, 3: checkNoAccess},
	)
	if !plan.Skip["live"] {
		t.Errorf("live block must be skipped: %+v", plan)
	}
	if !plan.Reupload["dead"] || !plan.Reupload["lost"] {
		t.Errorf("dead and lost blocks must be re-uploaded: %+v", plan)
	}
}
