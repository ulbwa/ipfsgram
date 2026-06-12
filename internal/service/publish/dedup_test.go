package publish

import (
	"reflect"
	"sort"
	"testing"

	"github.com/ulbwa/ipfsgram/internal/domain"
)

func TestPlanDedup(t *testing.T) {
	const (
		cidA = "cid-a"
		cidB = "cid-b"
		cidC = "cid-c"
		cidD = "cid-d"
	)

	tests := []struct {
		name       string
		existing   map[string]domain.Block
		statuses   map[int64]domain.CarStatus
		checks     map[int64]carCheck
		wantSkip   []string
		wantReup   []string
		wantDelete []int64
		wantStatus map[int64]domain.CarStatus
	}{
		{
			name: "published+ok skips",
			existing: map[string]domain.Block{
				cidA: {CID: []byte(cidA), CarID: 1},
				cidB: {CID: []byte(cidB), CarID: 1},
			},
			statuses:   map[int64]domain.CarStatus{1: domain.CarPublished},
			checks:     map[int64]carCheck{1: checkOK},
			wantSkip:   []string{cidA, cidB},
			wantStatus: map[int64]domain.CarStatus{},
		},
		{
			name: "deleted message reuploads and deletes car",
			existing: map[string]domain.Block{
				cidA: {CID: []byte(cidA), CarID: 2},
			},
			statuses:   map[int64]domain.CarStatus{2: domain.CarPublished},
			checks:     map[int64]carCheck{2: checkDeleted},
			wantReup:   []string{cidA},
			wantDelete: []int64{2},
			wantStatus: map[int64]domain.CarStatus{},
		},
		{
			name: "no_access marks status and reuploads",
			existing: map[string]domain.Block{
				cidA: {CID: []byte(cidA), CarID: 3},
			},
			statuses:   map[int64]domain.CarStatus{3: domain.CarPublished},
			checks:     map[int64]carCheck{3: checkNoAccess},
			wantReup:   []string{cidA},
			wantStatus: map[int64]domain.CarStatus{3: domain.CarNoBotAccess},
		},
		{
			name: "too_large marks status and reuploads",
			existing: map[string]domain.Block{
				cidA: {CID: []byte(cidA), CarID: 4},
			},
			statuses:   map[int64]domain.CarStatus{4: domain.CarPublished},
			checks:     map[int64]carCheck{4: checkTooLarge},
			wantReup:   []string{cidA},
			wantStatus: map[int64]domain.CarStatus{4: domain.CarTooLarge},
		},
		{
			name: "already-pending car reuploads without status change",
			existing: map[string]domain.Block{
				cidA: {CID: []byte(cidA), CarID: 5},
			},
			statuses:   map[int64]domain.CarStatus{5: domain.CarPending},
			checks:     map[int64]carCheck{},
			wantReup:   []string{cidA},
			wantStatus: map[int64]domain.CarStatus{},
		},
		{
			name: "mixed: live skip, deleted reupload",
			existing: map[string]domain.Block{
				cidA: {CID: []byte(cidA), CarID: 1},
				cidC: {CID: []byte(cidC), CarID: 6},
			},
			statuses: map[int64]domain.CarStatus{
				1: domain.CarPublished,
				6: domain.CarPublished,
			},
			checks: map[int64]carCheck{
				1: checkOK,
				6: checkDeleted,
			},
			wantSkip:   []string{cidA},
			wantReup:   []string{cidC},
			wantDelete: []int64{6},
			wantStatus: map[int64]domain.CarStatus{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planDedup(tt.existing, tt.statuses, tt.checks)

			if got := keys(plan.Skip); !equalStrings(got, tt.wantSkip) {
				t.Errorf("Skip = %v, want %v", got, tt.wantSkip)
			}
			if got := keys(plan.Reupload); !equalStrings(got, tt.wantReup) {
				t.Errorf("Reupload = %v, want %v", got, tt.wantReup)
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
