package talon

import (
	"fmt"
	"strconv"
	"time"

	influxdb "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

// https://github.com/grafana/grafana/issues/171#issuecomment-113533494
// https://github.com/influxdata/influxdb/issues/5930#issuecomment-226580852
// InfluxDB如果实现了lag功能，就能实现在一个panel里实现today vs yesterday，在此之前就只能用程序将昨天的数据收集存储到另一张表
type continuousQueries []continuousQuery

var cqs = continuousQueries{
	{
		// talon basic
		selectPart:    `sum("releaseCount_int")`,
		targetField:   "releaseCount",
		measurement:   "go_talon",
		condition:     `"type"='statistics'`,
		runBasicQuery: true,

		interval: 5 * time.Minute,
	},
	{
		// talon with tag
		selectPart:  `sum("captureCount_int")`,
		targetField: "captureCount",
		measurement: "go_talon",
		condition:   `"type"='statistics'`,
		tagNames:    []string{"topic"},

		interval: 5 * time.Minute,
	},
}

// Run interval for checking continuous queries. This should be set to the least common factor
// of the interval for running continuous queries.If you only aggregate continuous queries
// every minute, this should be set to 1 minute.
var runInterval = time.Minute

// talon滞后实时日志的时间差
var delay = 35 * time.Second

var timeRanges = []int{1, 7} // 以天为单位，比较1天前和7天前

func (t *Talon) continuousQueriesLoop() {
	tick := time.NewTicker(runInterval)
	for {
		select {
		case <-tick.C:
			go t.executeContinuousQueries()
		}
	}
}

type query struct {
	influxQL string
	tags     map[string]string
}

func (t *Talon) executeContinuousQueries() {
	now := time.Now().Add(-delay)

	bp, _ := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  "compare",
		Precision: t.getOpts().Influx.Precision,
	})

	for _, cq := range cqs {
		run, nextRun := cq.shouldRunContinuousQuery(now)
		if !run {
			continue
		}

		// We're about to run the query so store the current time closest to the nearest interval.
		// If all is going well, this time should be the same as nextRun.
		cq.hasRun = true
		cq.lastRun = now.Truncate(cq.interval)

		end := nextRun.Truncate(cq.interval)
		start := end.Add(-cq.interval)

		// basic query
		bq := fmt.Sprintf(`SELECT %s FROM "%s"`, cq.selectPart, cq.measurement)
		if cq.condition != "" {
			bq = fmt.Sprintf("%s WHERE %s", bq, cq.condition)
		}

		queries := []query{}
		if cq.runBasicQuery {
			queries = append(queries, query{influxQL: setTimeRange(bq, start, end), tags: map[string]string{"mark": "withoutAssignedTags"}})
		}

		for _, tagName := range cq.tagNames {
			tagValues, err := t.getTagValue(cq.measurement, tagName)
			if err != nil {
				log.WithError(err).Error("failed to getTagValue")
				continue
			}

			for _, tv := range tagValues {
				cond := fmt.Sprintf(`"%s"='%s'`, tagName, tv)
				q := fmt.Sprintf(`%s AND %s`, bq, cond)
				if cq.condition == "" {
					q = fmt.Sprintf(`%s WHERE %s`, bq, cond)
				}

				queries = append(queries, query{influxQL: setTimeRange(q, start, end), tags: map[string]string{tagName: tv}})
			}
		}

		for _, q := range queries {
			res, err := t.queryDB(q.influxQL)
			if err != nil {
				log.WithError(err).WithField("influxQL", q.influxQL).Error("faild to query influxdb")
				continue
			}

			if len(res) < 1 || len(res[0].Series) < 1 || len(res[0].Series[0].Values) < 1 || len(res[0].Series[0].Values[0]) < 2 {
				log.Warnf("empty result. cqResult:%v, influxQL:%s", res, q.influxQL)
				continue
			}

			val := res[0].Series[0].Values[0][1]
			// TODO 暂时支持int类型的聚合运算
			result, err := strconv.ParseInt(fmt.Sprintf("%v", val), 10, 64)
			if err != nil {
				log.WithError(err).Errorf("failed to ParseInt. influxQL:%s", q.influxQL)
				continue
			}

			fields := map[string]interface{}{
				cq.targetField: result,
			}

			pts := []*influxdb.Point{}
			for i := 0; i < len(timeRanges); {
				tr := timeRanges[i]
				q.tags["timeRange"] = strconv.Itoa(tr)
				pt, err := influxdb.NewPoint(cq.measurement, q.tags, fields, start.Add(time.Duration(tr)*time.Hour*24))
				if err != nil {
					log.WithError(err).Error("failed to new point")
					continue
				}
				pts = append(pts, pt)
				i++
			}
			bp.AddPoints(pts)
		}
	}

	t.writePoints(bp)
}
