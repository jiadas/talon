package talon

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"sync/atomic"

	"github.com/galaxydi/go-loghub"
	influxdb "github.com/influxdata/influxdb/client/v2"
	"github.com/jiadas/talon/app/config"
	log "github.com/sirupsen/logrus"
)

var maxBatchCount = 5000

type dbName string
type rpName string

type memoryPoint struct {
	point *influxdb.Point
	db    dbName
	rp    rpName
}

type Talon struct {
	sync.Mutex

	opts atomic.Value

	cursorMap map[int]string

	client            influxdb.Client
	batchIntervalTick <-chan time.Time
	memoryPointChan   chan memoryPoint
	failedBatchPChan  chan influxdb.BatchPoints

	// 统计从Loghub读取的日志数量
	captureCounter chan captured
	// 统计往InfluxDB写入的数据量
	releaseCounter chan int64

	logstore *sls.LogStore
}

func New(opts *config.Options) (t *Talon) {
	// Create a new HTTPClient
	client, err := newInfluxDBClient(opts.Influx)
	if err != nil {
		log.WithError(err).Fatal("failed to new influxdb client")
	}

	// Make sure that we can connect to InfluxDB
	_, _, err = client.Ping(5 * time.Second) // if this takes more than 5 seconds then influxdb is probably down
	if err != nil {
		log.WithError(err).Fatal("failed to connecting to InfluxDB")
	}

	project, err := sls.NewLogProject(opts.SLS.ProjectName, opts.SLS.Endpoint, opts.SLS.AccessKeyID, opts.SLS.AccessKeySecret)
	if err != nil {
		log.WithError(err).Fatal("failed to get project")
	}
	logstore, err := project.GetLogStore(opts.SLS.LogstoreName)
	if err != nil {
		log.WithError(err).Fatal("faild to get project")
	}

	t = &Talon{
		cursorMap: make(map[int]string),

		client:            client,
		batchIntervalTick: time.Tick(time.Duration(opts.Influx.BatchInterval) * time.Second),
		memoryPointChan:   make(chan memoryPoint, opts.MemQueueSize),
		failedBatchPChan:  make(chan influxdb.BatchPoints, 1000),

		captureCounter: make(chan captured, 1000),
		releaseCounter: make(chan int64, 1000),

		logstore: logstore,
	}

	t.swapOpts(opts)

	if !opts.IsOnlineEnvironment() {
		// 在dev测试时删除之前创建的measurements
		err = t.dropAllSeries()
		if err != nil {
			log.WithError(err).Error("failed to clean measurements in dev")
		}
		log.Info("complete clean measurements in dev")
	}

	go t.watchRemoteConfig()

	go t.handleBatch()

	go t.statistics()

	go t.continuousQueriesLoop()

	return
}

func (t *Talon) getOpts() *config.Options {
	return t.opts.Load().(*config.Options)
}

func (t *Talon) swapOpts(opts *config.Options) {
	t.opts.Store(opts)
}

func (t *Talon) Exit() {
	t.Lock()
	err := t.PersistMetadata()
	if err != nil {
		log.WithError(err).Error("failed to persist metadata")
	}
	t.Unlock()
}

func (t *Talon) getCursor(id int) (string, error) {
	// 在dev环境至消费5分钟的日志
	if !t.getOpts().IsOnlineEnvironment() {
		return t.logstore.GetCursor(id, fmt.Sprintf("%d", time.Now().Add(-5*time.Minute).Unix()))
	}

	if c, ok := t.cursorMap[id]; ok {
		return c, nil
	}
	// 默认从1分钟前的日志开始消费
	from := fmt.Sprintf("%d", time.Now().Add(-1*time.Minute).Unix())
	return t.logstore.GetCursor(id, from)
}

func (t *Talon) setCursor(id int, cursor string) {
	t.Lock()
	t.cursorMap[id] = cursor
	t.Unlock()
}

func (t *Talon) Capture() {
	// 查询指定的logstore有几个shard
	shardIDs, err := t.logstore.ListShards()
	if err != nil {
		log.WithError(err).Fatal("failed to list shards")
	}
	log.WithField("IDs", shardIDs).Info("list shards")

	for _, shardID := range shardIDs {
		go func(id int) {
			logger := log.WithField("shardID", id)
			// 指定游标
			cursor, err := t.getCursor(id)
			if err != nil {
				logger.WithError(err).Error("faild to get begin cursor")
				return // TODO 增加重试
			}

			endCursor, err := t.logstore.GetCursor(id, "end")
			if err != nil {
				logger.WithError(err).Error("faild to get end cursor")
				return // TODO 增加重试
			}

			var sema = make(chan struct{}, 500) // 限制同时只有500个并发

			for {
				logger = logger.WithFields(log.Fields{"endCursor": endCursor})

				gl, nextCursor, err := t.logstore.PullLogs(id, cursor, endCursor, t.getOpts().SLS.LogGroupCount)
				if err != nil {
					if err == io.ErrUnexpectedEOF {
						logger.WithError(err).Warn("failed to ReadAll in GetLogsBytes")
					} else {
						logger.WithError(err).Error("faild to PullLogs")
					}
					time.Sleep(time.Millisecond * 100)
					continue // 此处continue相当于循环重试
				}

			GroupLoop:
				for _, logGroup := range gl.LogGroups {
					// 过滤掉测试环境的日志源
					for _, igs := range t.getOpts().SLS.IgnoreSources {
						if logGroup.GetSource() == igs {
							continue GroupLoop
						}
					}

					sema <- struct{}{}
					go func(group *sls.LogGroup) {
						t.release(group)
						<-sema
					}(logGroup)
				}

				cursor = nextCursor
				t.setCursor(id, cursor)

				// cursor已经追上了endCursor
				if cursor == endCursor {
					if !t.getOpts().IsOnlineEnvironment() {
						logger.Info("finished the sample collection")
						return
					}
					time.Sleep(30 * time.Second)
					freshEndCursor, err := t.logstore.GetCursor(id, "end")
					if err != nil {
						logger.WithField("cursor", cursor).WithError(err).Error("failed to refresh end cursor")
					} else {
						logger.WithFields(log.Fields{"cursor": cursor, "freshEndCursor": freshEndCursor}).Info("----------refresh end cursor----------")
						endCursor = freshEndCursor
					}
				}
			}
		}(shardID)
	}
}

func (t *Talon) isTarget(topicName string) bool {
	topic, ok := t.getOpts().Topics.TopicMap[topicName]
	if ok {
		return !topic.Pause
	}

	return ok
}

func unionSlice(a, b []string) []string {
	r := make([]string, 0, len(a)+len(b))
	r = append(r, a...)
	return append(r, b...)
}

func (t *Talon) release(g *sls.LogGroup) {
	// 不同的topic存储在不同的measurement
	topicName := g.GetTopic()
	if !t.isTarget(topicName) {
		return
	}

	// 统计不同topic采集的日志量
	t.captureCounter <- captured{topic: topicName, logCount: int64(len(g.Logs))}

	topic := t.getOpts().Topics.TopicMap[topicName]
	tagList := unionSlice(t.getOpts().Topics.CommonTagList, topic.TagList)
	intList := unionSlice(t.getOpts().Topics.CommonIntList, topic.IntList)
	ignoreList := unionSlice(t.getOpts().Topics.CommonIgnoreList, topic.IgnoreList)
	// 将include的key从commonIgnoreList中将移除
	for _, include := range topic.IncludeList {
		for i, ignore := range ignoreList {
			if include == ignore {
				// Delete without preserving order
				ignoreList[i] = ignoreList[len(ignoreList)-1]
				ignoreList = ignoreList[:len(ignoreList)-1]
			}
		}
	}

LogLoop:
	for _, l := range g.Logs {
		fields := map[string]interface{}{
			"preselected": "always carry", // 每个point都会携带这个k-v，一来可以保证写到InfluxDB的point至少有一个field，二来可以在Grafana的图表统计中用来计数：select count("preselected")
		}

		var err error
		timestamp := time.Now()
		for _, c := range l.Contents {
			// to avoid empty key
			if c.GetKey() == "" {
				continue
			}

			// dump the log which contains dump Key
			for _, dumpKey := range topic.DumpList {
				if c.GetKey() == dumpKey {
					continue LogLoop
				}
			}

			fields[c.GetKey()] = c.GetValue()

			// 用日志里打印的时间戳
			if c.GetKey() == "cur_time" || c.GetKey() == "time" {
				timestamp, err = time.Parse(time.RFC3339Nano, c.GetValue())
				if err != nil {
					// 时间解析错误就用当前时间
					timestamp = time.Now()
					log.WithError(err).WithFields(log.Fields{
						"topic":     topicName,
						"logSource": g.GetSource(),
					}).Error("faild to parse time")
				}
			}
		}

		var shouldPickup bool
		for pk, pv := range topic.PickupMap {
			v, ok := fields[pk]
			if ok && pv == v {
				shouldPickup = true
			}
		}
		if !shouldPickup && len(topic.PickupMap) > 0 {
			continue LogLoop
		}

		// split
		// 将split放在所有带delete操作的前面，避免fields里没有split对应的key
		measurement := topicName
		db := dbName(t.getOpts().Influx.Database)
		var rp rpName
		for splitKey, splitValue := range topic.SplitMap {
			if fv, ok := fields[splitKey]; ok && splitValue.FieldValue == fv {
				measurement = splitValue.MeasurementName
				rp = rpName(splitValue.RetentionPolicy)

				if splitValue.Database != "" {
					db = dbName(splitValue.Database)
				}
			}
		}

		// add tags
		// InfluxDB 要求 Tag keys and tag values are both strings.
		tags := map[string]string{
			"source": g.GetSource(),
		}

		for _, tag := range tagList {
			if v, ok := fields[tag]; ok {
				tagValue := fmt.Sprintf("%v", v)
				tags[tag] = tagValue
				// 删除fields中tagKey对应的value，不然就会出现tagKey和fieldKey重名的情况，这样的话查询条件里需要用到tag时，必须用::tag来标识tag
				delete(fields, tag)
			}
		}

		// convert to int
		for _, intKey := range intList {
			if v, ok := fields[intKey]; ok {
				vs := fmt.Sprintf("%v", v)
				intValue, err := strconv.ParseInt(vs, 10, 64)
				if err != nil {
					log.WithError(err).WithFields(log.Fields{"intKey": intKey, "intValue": vs}).Error("failed to ParseInt")
					continue
				}
				// TODO 在增加map元素的同时删除可能会有问题
				// 既然在key后面加int后缀，那么原来的key就可以删掉了，避免重复存储
				delete(fields, intKey)
				// 在key后面加int后缀
				intKey = fmt.Sprintf("%s_int", intKey)
				fields[intKey] = intValue
			}
		}

		// ignore filter keys
		for _, ignore := range ignoreList {
			if _, ok := fields[ignore]; ok {
				delete(fields, ignore)
			}
		}

		// Create a point and add to batch
		pt, err := influxdb.NewPoint(measurement, tags, fields, timestamp)
		if err != nil {
			log.WithError(err).Errorf("failed to NewPoint: measurement=%s, tags=%v, fields=%v", measurement, tags, l.Contents)
			continue
		}

		t.memoryPointChan <- memoryPoint{
			point: pt,
			db:    db,
			rp:    rp,
		}
	}
}

type batchMap map[dbName]map[rpName]*influxdb.BatchPoints

func (t *Talon) handleBatch() {
	var batchM = make(batchMap)
	for {
		select {
		case mp := <-t.memoryPointChan:
			var bp *influxdb.BatchPoints
			rpmap, ok := batchM[mp.db]
			if !ok {
				rpmap = map[rpName]*influxdb.BatchPoints{}
				batchM[mp.db] = rpmap
			}
			bp, ok = rpmap[mp.rp]
			if !ok || *bp == nil {
				b, err := newBatchPoints(t.getOpts().Influx.Precision, string(mp.db), string(mp.rp))
				if err != nil {
					log.WithError(err).Error("failed to new BatchPoints")
					t.memoryPointChan <- mp
					break
				}
				rpmap[mp.rp] = &b
				bp = &b
			}

			(*bp).AddPoint(mp.point)

			if len((*bp).Points()) >= t.getOpts().Influx.BatchCount || len((*bp).Points()) >= maxBatchCount {
				t.writePoints(*bp)
				*bp = nil
			}
		case <-t.batchIntervalTick:
			for _, rpmap := range batchM {
				for _, bp := range rpmap {
					if *bp != nil {
						t.writePoints(*bp)
						*bp = nil
					}
				}
			}

		case failedBatchP := <-t.failedBatchPChan:
			if failedBatchP == nil {
				break
			}
			// BatchPoints写入失败多半是因为Points太多，导致超时
			// 将Points重新分批之后再次写入InfluxDB
			points := failedBatchP.Points()
			length := len(points)
			for start, end := 0, length/2; end <= length && start < length; {
				newBatchP, err := newBatchPoints(t.getOpts().Influx.Precision, failedBatchP.Database(), failedBatchP.RetentionPolicy())
				if err != nil {
					log.WithError(err).Error("failed to newBatchPoints in handleFailed")
					continue
				}
				newBatchP.AddPoints(points[start:end])
				t.writePoints(newBatchP)
				log.WithField("rewritePoints", len(newBatchP.Points())).Infof("rewrite failed points succeeded. start=%v,end=%v", start, end)
				start, end = end, length
			}
		}
	}
}

func (t *Talon) writePoints(bp influxdb.BatchPoints) {
	if bp == nil || len(bp.Points()) <= 0 {
		return
	}

	err := t.client.Write(bp)
	if err != nil {
		// 丢到重试的channel里
		t.failedBatchPChan <- bp
		log.WithError(err).WithField("failedPoints", len(bp.Points())).Error("failed to write BatchPoints")
		return
	}

	t.releaseCounter <- int64(len(bp.Points()))
}
