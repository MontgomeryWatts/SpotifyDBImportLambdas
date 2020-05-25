package dynamodb

import (
	"time"
)

type TrackingItem struct {
	EntityURI   string
	LastUpdated time.Time
}
