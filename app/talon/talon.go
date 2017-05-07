package talon

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

type Talon struct {
	sync.Mutex // TODO: we should clean up all of these locks

	client              influxdb.Client
	precision, database string
	tagList             []string
	intList             []string
	ignoreList          []string
	batchP              influxdb.BatchPoints
	batchInterval       time.Duration
	batchCount          int

	logstore *sls.LogStore
}

func New() (t *Talon) {
	// Create a new HTTPClient
	client, err := newInfluxDBClient(config.Options.Influx)
	if err != nil {
		log.WithError(err).Fatal("failed to new influxdb client")
	}

	// Make sure that we can connect to InfluxDB
	_, _, err = client.Ping(5 * time.Second) // if this takes more than 5 seconds then influxdb is probably down
	if err != nil {
		log.WithError(err).Fatal("failed to connecting to InfluxDB")
	}

	project, err := sls.NewLogProject(config.Options.SLS.ProjectName, config.Options.SLS.Endpoint, config.Options.SLS.AccessKeyID, config.Options.SLS.AccessKeySecret)
	if err != nil {
		log.WithError(err).Fatal("failed to get project")
	}
	logstore, err := project.GetLogStore(config.Options.SLS.LogstoreName)
	if err != nil {
		log.WithError(err).Fatal("faild to get project")
	}

	t = &Talon{
		client:        client,
		precision:     config.Options.Influx.Precision,
		database:      config.Options.Influx.Database,
		tagList:       config.Options.Influx.TagList,
		intList:       config.Options.Influx.IntList,
		ignoreList:    config.Options.Influx.IgnoreList,
		batchInterval: time.Duration(config.Options.Influx.BatchInterval) * time.Second,
		batchCount:    config.Options.Influx.BatchCount,

		logstore: logstore,
	}

	go t.handleBatch()

	return
}

func (t *Talon) Capture() (err error) {
	// 查询指定的logstore有几个shard
	shardIDs, err := t.logstore.ListShards()
	if err != nil {
		return
	}
	log.WithField("IDs", shardIDs).Info("list shards")

	// TODO 去掉wg
	var wg sync.WaitGroup
	for _, shardID := range shardIDs {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 指定游标
			// TODO 暂时只获取距启动前一个小时内的日志
			cursor, err := t.logstore.GetCursor(id, fmt.Sprintf("%d", time.Now().Add(-1*time.Hour).Unix()))
			if err != nil {
				log.WithError(err).WithField("shardID", id).Error("faild to get begin cursor")
				return // TODO 增加重试
			}
			endCursor, err := t.logstore.GetCursor(id, "end")
			if err != nil {
				log.WithError(err).WithField("shardID", id).Error("faild to get end cursor")
				return // TODO 增加重试
			}

			for cursor != "" {
				log.WithField("cursor", cursor).Info("mark cursor")

				// TODO 暂时只拿5个日志组
				gl, nextCursor, err := t.logstore.PullLogs(0, cursor, endCursor, 5)
				if err != nil {
					log.WithError(err).Error("faild to PullLogs")
					continue // 此处continue相当于循环重试
				}

				for _, g := range gl.LogGroups {
					go t.release(g)
				}

				cursor = nextCursor
			}
		}(shardID)
	}
	wg.Wait()
	return // TODO 从sls消费的速度大于日志产生的速度，但是不应该结束消费
}

func (t *Talon) release(g *sls.LogGroup) {
	measurement := g.GetTopic()
	if measurement != "go_feed_rpc" && measurement != "go_order_paid_callback" {
		return
	}

	tags := map[string]string{
		"source": g.GetSource(),
	}

	for _, l := range g.Logs {
		fields := map[string]interface{}{
			"source": g.GetSource(),
		}

		var err error
		timestamp := time.Now()
		for _, c := range l.Contents {
			fields[c.GetKey()] = c.GetValue()

			// 用日志里打印的时间戳
			if c.GetKey() == "cur_time" || c.GetKey() == "time" {
				timestamp, err = time.Parse(time.RFC3339Nano, c.GetValue())
				if err != nil {
					// 时间解析错误就用当前时间
					timestamp = time.Now()
					log.WithError(err).WithField("source", g.GetSource()).Error("faild to parse time")
				}
			}
		}

		// convert to int
		for _, intKey := range t.intList {
			if v, ok := fields[intKey]; ok {
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
		for _, tag := range t.tagList {
			if tagValue, ok := getTag(fields, tag); ok {
				tags[tag] = tagValue
			}
		}

		// ignore filter keys
		for _, ignore := range t.ignoreList {
			if _, ok := fields[ignore]; ok {
				delete(fields, ignore)
			}
		}

		// Create a point and add to batch
		pt, err := influxdb.NewPoint(measurement, tags, fields, timestamp)
		if err != nil {
			log.WithError(err).Error("failed to NewPoint")
			continue
		}

		err = t.addPoint(pt)
		if err != nil {
			log.WithError(err).Error("failed to addPoint")
		}
	}
}

func (t *Talon) addPoint(pt *influxdb.Point) (err error) {
	t.Lock()
	defer t.Unlock()
	if t.batchP == nil {
		err = t.newBatchPoints()
		if err != nil {
			return fmt.Errorf("Error creating new batch: %v", err)
		}
	}
	t.batchP.AddPoint(pt)

	// if the number of batch points are less than the batch size then we don't need to write them yet
	if len(t.batchP.Points()) < t.batchCount {
		return nil
	}
	return t.writePoints()
}

func (t *Talon) writePoints() (err error) {
	err = t.client.Write(t.batchP)
	if err != nil {
		return err
	}
	t.batchP = nil
	return nil
}

func (t *Talon) handleBatch() {
	if t.batchInterval == 0 || t.batchCount == 0 {
		// we don't need to process this if the interval is 0
		return
	}
	for {
		time.Sleep(t.batchInterval)
		t.Lock()
		if t.batchP != nil {
			t.writePoints()
		}
		t.Unlock()
	}
}

// Try to return a tag
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
