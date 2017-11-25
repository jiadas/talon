package talon

import (
	"fmt"
	"strings"
	"time"
)

type continuousQuery struct {
	selectPart  string
	targetField string
	measurement string
	condition   string
	tagNames    []string // 存储tag的名字

	runBasicQuery bool

	hasRun   bool
	lastRun  time.Time
	interval time.Duration
}

func (cq *continuousQuery) shouldRunContinuousQuery(now time.Time) (bool, time.Time) {
	if cq.hasRun {
		nextRun := cq.lastRun.Add(cq.interval)
		if nextRun.UnixNano() <= now.UnixNano() {
			return true, nextRun
		}
	} else {
		return true, now
	}
	return false, cq.lastRun
}

func setTimeRange(q string, start, end time.Time) string {
	cond := fmt.Sprintf("time >= '%s' AND time < '%s'", start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))
	if !strings.Contains(strings.ToUpper(q), "WHERE") {
		return fmt.Sprintf("%s WHERE %s", q, cond)
	}
	return fmt.Sprintf("%s AND %s", q, cond)
}
