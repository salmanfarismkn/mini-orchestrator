package autoscaler

import (
	"testing"
	"time"
)

func TestLimitScaleStep(t *testing.T) {
	tests := []struct {
		current int
		desired int
		maxStep int
		want    int
	}{
		{
			current: 3,
			desired: 10,
			maxStep: 2,
			want:    5,
		},
		{
			current: 10,
			desired: 2,
			maxStep: 2,
			want:    8,
		},
		{
			current: 5,
			desired: 6,
			maxStep: 2,
			want:    6,
		},
	}

	for _, tt := range tests {
		got := limitScaleStep(
			tt.current,
			tt.desired,
			tt.maxStep,
		)

		if got != tt.want {
			t.Fatalf(
				"expected %d, got %d",
				tt.want,
				got,
			)
		}
	}
}

func TestStableObservation(t *testing.T) {
	observations := make(map[string]observation)

	now := time.Now()

	if stableObservation(
		observations,
		"service-1",
		5,
		3,
		now,
	) {
		t.Fatal("first observation should not scale")
	}

	if stableObservation(
		observations,
		"service-1",
		5,
		3,
		now.Add(time.Second),
	) {
		t.Fatal("second observation should not scale")
	}

	if !stableObservation(
		observations,
		"service-1",
		5,
		3,
		now.Add(2*time.Second),
	) {
		t.Fatal("third observation should scale")
	}
}