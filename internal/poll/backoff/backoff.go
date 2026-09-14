package backoff

import (
	"time"
)

type ConnectorHistory struct {
	lastRestartAttemptTime time.Time
}
