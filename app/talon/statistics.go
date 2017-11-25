package talon

import (
	"time"

	log "github.com/sirupsen/logrus"
)

type captured struct {
	topic    string
	logCount int64
}

func (t *Talon) statistics() {
	statisticsTick := time.Tick(30 * time.Second)
	var releaseCount int64
	capturedCountMap := map[string]int64{}
	for {
		select {
		case rc := <-t.releaseCounter:
			releaseCount += rc
		case cc := <-t.captureCounter:
			capturedCountMap[cc.topic] += cc.logCount
		case <-statisticsTick:
			log.WithFields(log.Fields{
				"type":         "statistics",
				"releaseCount": releaseCount,
			}).Info("release count in 30 second")

			for topicName, captureCount := range capturedCountMap {
				log.WithFields(log.Fields{
					"type":         "statistics",
					"captureCount": captureCount,
					"topic":        topicName,
				}).Info("capture count in 30 second")
			}

			releaseCount = 0
			capturedCountMap = map[string]int64{}
		}
	}
}
