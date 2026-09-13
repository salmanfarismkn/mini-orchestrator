package autoscaler

import "time"

type observation struct {
	desiredReplicas int
	count           int
	lastSeen        time.Time
}
