package config

import (
	"encoding/json"
	"flag"
	"strings"

	"github.com/jiadas/talon/app/shared/etcd"
	log "github.com/sirupsen/logrus"
)

type SLS struct {
	ProjectName   string   `json:"project_name"`
	LogstoreName  string   `json:"logstore_name"`
	LogGroupCount int      `json:"log_group_count"` // 一次从shard里获取的loggroup数目，范围为 0~1000
	IgnoreSources []string `json:"ignore_sources"`  // 需要过滤的日志源

	Endpoint        string `json:"endpoint"`
	AccessKeyID     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
}

type Influx struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Database  string `json:"database"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	UseHTTPS  bool   `json:"use_https"`
	Precision string `json:"precision"`
	Timeout   int64  `json:"timeout"` // 单位秒

	// Batching
	// Defaults to 5s. 单位秒
	BatchInterval int64 `json:"batch_interval"`
	// Defaults to 2000.
	// InfluxData recommends batch sizes of 5,000-10,000 points,
	// although different use cases may be better served by significantly smaller or larger batches.
	BatchCount int `json:"batch_count"`
}

type Topics struct {
	CommonTagList    []string `json:"common_tag_list"`    // 所有日志主题都需要作为tag的key
	CommonIntList    []string `json:"common_int_list"`    // 所有日志主题都需要转换成int类型的key
	CommonIgnoreList []string `json:"common_ignore_list"` // 所有日志主题都需要过滤的key
	TopicMap         TopicMap `json:"topic_map"`          // 指定要消费的日志主题
}

type TopicMap map[string]Topic

type Topic struct {
	Pause       bool                  `json:"pause"`        // 暂停消费改topic的日志
	TagList     []string              `json:"tag_list"`     // 需要作为tag的key
	IntList     []string              `json:"int_list"`     // 需要转换成int类型的key
	IgnoreList  []string              `json:"ignore_list"`  // 需要过滤的key
	IncludeList []string              `json:"include_list"` // 需要采集的key，主要作用是将key从commonIgnoreList中将移除
	DumpList    []string              `json:"dump_list"`    // 包含指定的key，整条日志都丢弃
	PickupMap   map[string]string     `json:"pickup_map"`   // 只抓取包含指定key和value的日志
	SplitMap    map[string]SplitValue `json:"split_map"`    // 需要将包含指定fieldKey和fieldValue的日志，单独存储到另外的measurement中去。key:fieldKey
}

type SplitValue struct {
	FieldValue      string `json:"field_value"`
	Database        string `json:"database"`         // 需要单独存储到哪个database，默认为talon
	MeasurementName string `json:"measurement_name"` // 需要单独存储到哪个measurement，不能为空
	RetentionPolicy string `json:"retention_policy"` // 需要单独存储到哪个rp，默认为default
}

type Options struct {
	Environment string `json:"environment"`
	LogDir      string `json:"log_dir"`
	DataPath    string `json:"data_path"`

	Topics       Topics `json:"topics"` // 指定要消费的日志主题
	MemQueueSize int64  `json:"mem_queue_size"`

	SLS *SLS `json:"sls"`

	Influx *Influx `json:"influx"`
}

const (
	BasicKey  = "your/key/path/in/etcd/to/basic"
	TopicsKey = "your/key/path/in/etcd/to/topics"
)

func NewOptions() *Options {
	var opts Options

	basicValue, err := etcd.Get(BasicKey)
	if err != nil {
		log.WithError(err).Fatal("failed to get basic config from etcd")
	}
	if err := json.NewDecoder(strings.NewReader(basicValue)).Decode(&opts); err != nil {
		log.WithError(err).Fatal("failed to unmarshal basic config")
	}

	var topics Topics
	topicsValue, err := etcd.Get(TopicsKey)
	if err != nil {
		log.WithError(err).Fatal("failed to get topics config from etcd")
	}
	if err := json.NewDecoder(strings.NewReader(topicsValue)).Decode(&topics); err != nil {
		log.WithError(err).Fatal("failed to unmarshal topics config")
	}

	opts.Topics = topics

	return &opts
}

// 通过命令行参数设置Options，目前只支持设置环境参数
func (opts *Options) SetByFlags() {
	evn := flag.String("environment", opts.Environment, "specify the running environment('dev' or 'live')")
	ihost := flag.String("ihost", opts.Influx.Host, "specify the influx host")
	idb := flag.String("idb", opts.Influx.Database, "specify the influx database")
	iuser := flag.String("iusername", opts.Influx.Username, "specify the influx username")
	ipw := flag.String("ipassword", opts.Influx.Password, "specify the influx password")

	flag.Parse()

	if *evn == "dev" {
		opts.Environment = *evn
	}

	if !opts.IsOnlineEnvironment() {
		opts.Influx.Host = *ihost
		opts.Influx.Database = *idb
		opts.Influx.Username = *iuser
		opts.Influx.Password = *ipw
	}

}

func (opts *Options) IsOnlineEnvironment() bool {
	if opts.Environment == "live" {
		return true
	}
	return false
}
