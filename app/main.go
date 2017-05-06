package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/galaxydi/go-loghub"
	influxdb "github.com/influxdata/influxdb/client/v2"

	"github.com/jiadas/talon/app/config"
)

func main() {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})

	// Create a new HTTPClient
	c, err := newInfluxDBClient(config.Options.Influx)
	if err != nil {
		log.WithError(err).Fatal("failed to new influxdb client")
	}

	project, err := sls.NewLogProject(config.Options.SLS.ProjectName, config.Options.SLS.Endpoint, config.Options.SLS.AccessKeyID, config.Options.SLS.AccessKeySecret)
	if err != nil {
		log.WithError(err).Fatal("failed to get project")
	}
	logstore, err := project.GetLogStore(config.Options.SLS.LogstoreName)
	if err != nil {
		log.WithError(err).Fatal("faild to get project")
	}

	// 查询指定的logstore有几个shard
	shardIDs, err := logstore.ListShards()
	log.WithField("IDs", shardIDs).Info("list shards")

	var wg sync.WaitGroup
	for _, shardID := range shardIDs {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 指定游标
			// TODO 暂时只获取距启动前一个小时内的日志
			//cursor, err := logstore.GetCursor(id, fmt.Sprintf("%d", time.Now().Add(-1*time.Hour).Unix()))
			cursor, err := logstore.GetCursor(id, fmt.Sprintf("%d", time.Now().Add(-4*time.Hour).Unix()))
			if err != nil {
				log.WithError(err).Fatal("faild to get begin cursor")
			}
			endCursor, err := logstore.GetCursor(id, "end")
			if err != nil {
				log.WithError(err).Fatal("faild to get end cursor")
			}

			for cursor != "" {
				// TODO 暂时只拿5个日志组
				gl, nextCursor, err := logstore.PullLogs(0, cursor, endCursor, 5)
				if err != nil {
					log.WithError(err).Error("faild to PullLogs")
					continue
				}

				// Create a new point batch
				bp, err := newBatchPoints()
				if err != nil {
					log.WithError(err).Fatal("failed to new influxdb batch")
				}

				for _, g := range gl.LogGroups {
					measurement := g.GetTopic()
					if measurement != "go_feed_rpc" && measurement != "go_order_paid_callback" {
						continue
					}
					tags := map[string]string{
						"source": g.GetSource(),
					}
					for _, l := range g.Logs {
						// Create a point and add to batch
						fields := map[string]interface{}{
							"source": g.GetSource(),
						}
						t := time.Now()
						for _, c := range l.Contents {
							fields[c.GetKey()] = c.GetValue()
							if c.GetKey() == "cur_time" || c.GetKey() == "time" {
								t, err = time.Parse(time.RFC3339Nano, c.GetValue())
								if err != nil {
									log.WithError(err).WithField("source", g.GetSource()).Error("faild to parse time")
								}
							}
						}

						// convert to int
						for _, intKey := range config.Options.Influx.Ints {
							v, ok := fields[intKey]
							if ok {
								vs := fmt.Sprintf("%s", v)
								result, err := strconv.ParseInt(vs, 10, 64)
								if err != nil {
									log.WithError(err).WithFields(log.Fields{"intKey": intKey, "intValue": vs}).Error("failed to ParseInt")
									continue
								}
								fields[intKey] = result
							}
						}

						// add tags
						for _, tag := range config.Options.Influx.Tags {
							if tagValue, ok := getTag(fields, tag); ok {
								tags[tag] = tagValue
							}
						}

						pt, err := influxdb.NewPoint(measurement, tags, fields, t)
						if err != nil {
							log.WithError(err).Error("failed to NewPoint")
							continue
						}
						if bp == nil {
							bp, err = newBatchPoints()
							if err != nil {
								log.WithError(err).Fatal("failed to new influxdb batch")
							}
						}
						bp.AddPoint(pt)
						if len(bp.Points()) >= config.Options.Influx.BatchCount {
							err = c.Write(bp)
							if err != nil {
								log.WithError(err).Error("failed to write into influx")
								continue
							}
							bp = nil
						}
					}
				}
				if bp != nil {
					err = c.Write(bp)
					if err != nil {
						log.WithError(err).Error("failed to write into influx")
					}
				}

				cursor = nextCursor
			}
		}(shardID)
	}
	wg.Wait()
}

// Returns an influxdb client
func newInfluxDBClient(config *config.Influx) (influxdb.Client, error) {
	protocol := "http"
	if config.UseHTTPS {
		protocol = "https"
	}
	return influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr:     fmt.Sprintf("%s://%s:%d", protocol, config.Host, config.Port),
		Username: config.Username,
		Password: config.Password,
		Timeout:  time.Duration(config.Timeout) * time.Second,
	})
}

func newBatchPoints() (influxdb.BatchPoints, error) {
	return influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  config.Options.Influx.Database,
		Precision: config.Options.Influx.Precision,
	})
}

// Try to return a field from logrus
// Taken from Sentry adapter (from https://github.com/evalphobia/logrus_sentry)
func getTag(f map[string]interface{}, key string) (tag string, ok bool) {
	v, ok := f[key]
	if !ok {
		return "", false
	}
	switch vs := v.(type) {
	case fmt.Stringer:
		return vs.String(), true
	case string:
		return vs, true
	case byte:
		return string(vs), true
	case int:
		return strconv.FormatInt(int64(vs), 10), true
	case int32:
		return strconv.FormatInt(int64(vs), 10), true
	case int64:
		return strconv.FormatInt(vs, 10), true
	case uint:
		return strconv.FormatUint(uint64(vs), 10), true
	case uint32:
		return strconv.FormatUint(uint64(vs), 10), true
	case uint64:
		return strconv.FormatUint(vs, 10), true
	default:
		return "", false
	}
	return "", false
}
