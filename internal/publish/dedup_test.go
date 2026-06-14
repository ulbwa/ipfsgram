// dedup_test.go — table tests for the pure planDedup decision.

package publish

import (
	"reflect"
	"sort"
	"testing"

	"github.com/ulbwa/ipfsgram/internal/store"
)

func TestPlanDedup(t *testing.T) {
	const (
		cidA = "cid-a"
		cidB = "cid-b"
		cidC = "cid-c"
	)

	tests := []struct {
		name       string
		existing   map[string]store.BlockRef
		statuses   map[int64]store.CarStatus
		checks     map[int64]carCheck
		wantSkip   []string
		wantDelete []int64
		wantStatus map[int64]store.CarStatus
	}{
		{
			name: "published+ok skips",
			existing: map[string]store.BlockRef{
				cidA: {CID: []byte(cidA), CarID: 1},
				cidB: {CID: []byte(cidB), CarID: 1},
			},
			statuses:   map[int64]store.CarStatus{1: store.CarPublished},
			checks:     map[int64]carCheck{1: checkOK},
			wantSkip:   []string{cidA, cidB},
			wantStatus: map[int64]store.CarStatus{},
		},
		{
			name: "deleted message reuploads and deletes car",
			existing: map[string]store.BlockRef{
				cidA: {CID: []byte(cidA), CarID: 2},
			},
			statuses:   map[int64]store.CarStatus{2: store.CarPublished},
			checks:     map[int64]carCheck{2: checkDeleted},
			wantDelete: []int64{2},
			wantStatus: map[int64]store.CarStatus{},
		},
		{
			name: "no_access marks status and reuploads",
			existing: map[string]store.BlockRef{
				cidA: {CID: []byte(cidA), CarID: 3},
			},
			statuses:   map[int64]store.CarStatus{3: store.CarPublished},
			checks:     map[int64]carCheck{3: checkNoAccess},
			wantStatus: map[int64]store.CarStatus{3: store.CarNoBotAccess},
		},
		{
			name: "too_large marks status and reuploads",
			existing: map[string]store.BlockRef{
				cidA: {CID: []byte(cidA), CarID: 4},
			},
			statuses:   map[int64]store.CarStatus{4: store.CarPublished},
			checks:     map[int64]carCheck{4: checkTooLarge},
			wantStatus: map[int64]store.CarStatus{4: store.CarTooLarge},
		},
		{
			name: "already-pending car reuploads without status change",
			existing: map[string]store.BlockRef{
				cidA: {CID: []byte(cidA), CarID: 5},
			},
			statuses:   map[int64]store.CarStatus{5: store.CarPending},
			checks:     map[int64]carCheck{},
			wantStatus: map[int64]store.CarStatus{},
		},
		{
			name: "mixed: live skip, deleted reupload",
			existing: map[string]store.BlockRef{
				cidA: {CID: []byte(cidA), CarID: 1},
				cidC: {CID: []byte(cidC), CarID: 6},
			},
			statuses: map[int64]store.CarStatus{
				1: store.CarPublished,
				6: store.CarPublished,
			},
			checks: map[int64]carCheck{
				1: checkOK,
				6: checkDeleted,
			},
			wantSkip:   []string{cidA},
			wantDelete: []int64{6},
			wantStatus: map[int64]store.CarStatus{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planDedup(tt.existing, tt.statuses, tt.checks)

			if got := keys(plan.Skip); !equalStrings(got, tt.wantSkip) {
				t.Errorf("Skip = %v, want %v", got, tt.wantSkip)
			}
			gotDelete := append([]int64(nil), plan.DeleteCars...)
			sort.Slice(gotDelete, func(i, j int) bool { return gotDelete[i] < gotDelete[j] })
			want := append([]int64(nil), tt.wantDelete...)
			sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
			if len(gotDelete) != len(want) || (len(want) > 0 && !reflect.DeepEqual(gotDelete, want)) {
				t.Errorf("DeleteCars = %v, want %v", gotDelete, want)
			}
			if !reflect.DeepEqual(plan.SetStatus, tt.wantStatus) {
				t.Errorf("SetStatus = %v, want %v", plan.SetStatus, tt.wantStatus)
			}
		})
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	b = append([]string(nil), b...)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
