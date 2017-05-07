package config

type SLS struct {
	ProjectName  string `json:"project_name"`
	LogstoreName string `json:"logstore_name"`

	Endpoint        string `json:"endpoint"`
	AccessKeyID     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
}

type Influx struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Database    string `json:"database"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	UseHTTPS    bool   `json:"use_https"`
	Measurement string `json:"measurement"`
	Precision   string `json:"precision"`
	Timeout     int64  `json:"timeout"` // 单位秒

	TagList    []string `json:"tag_list"`    // 需要作为tag的key
	IntList    []string `json:"int_list"`    // 需要转换成int类型的key
	IgnoreList []string `json:"ignore_list"` // 需要过滤的key

	// Batching
	BatchInterval int64 `json:"batch_interval"` // Defaults to 5s. 单位秒
	BatchCount    int   `json:"batch_count"`    // Defaults to 200.
}

var Options struct {
	DataPath string `json:"data_path"`

	SLS *SLS `json:"sls"`

	Influx *Influx `json:"influx"`
}

func init() {
	s := &SLS{
		ProjectName:  "demo",
		LogstoreName: "json",

		Endpoint:        "cn-hangzhou.log.aliyuncs.com",
		AccessKeyID:     "xxxx",
		AccessKeySecret: "xxx",
	}

	i := &Influx{
		Host:      "loca1host",
		Port:      8086,
		Database:  "demo",
		Username:  "admin",
		Password:  "xxxx",
		Precision: "ns",
		Timeout:   1,

		TagList:    []string{"level", "api", "type"},
		IntList:    []string{"count", "amount", "lantency"},
		IgnoreList: []string{"body", "input", "output"},

		BatchInterval: 5,
		BatchCount:    200,
	}

	Options.DataPath = "/var/data/"
	Options.SLS = s
	Options.Influx = i
}
