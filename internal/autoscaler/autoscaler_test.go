package autoscaler

import (
	"math"
	"testing"
)

func TestDesiredReplicas(t *testing.T) {
	tests := []struct {
		name       string
		totalUsage float64
		request    int
		target     float64
		min        int
		max        int
		want       int
	}{
		{
			name:       "scale up",
			totalUsage: 1400,
			request:    500,
			target:     70,
			min:        1,
			max:        10,
			want:       4,
		},
		{
			name:       "respect minimum",
			totalUsage: 100,
			request:    500,
			target:     70,
			min:        2,
			max:        10,
			want:       2,
		},
		{
			name:       "respect maximum",
			totalUsage: 10000,
			request:    500,
			target:     70,
			min:        1,
			max:        5,
			want:       5,
		},
	}

	for _, tt := range tests {
		got := int(math.Ceil(
			tt.totalUsage /
				(float64(tt.request) * tt.target / 100),
		))

		if got < tt.min {
			got = tt.min
		}

		if got > tt.max {
			got = tt.max
		}

		if got != tt.want {
			t.Fatalf(
				"%s: expected %d, got %d",
				tt.name,
				tt.want,
				got,
			)
		}
	}
}
