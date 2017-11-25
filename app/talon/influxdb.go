package talon

import (
	"fmt"
	"time"

	influxdb "github.com/influxdata/influxdb/client/v2"
	"github.com/jiadas/talon/app/config"
)

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

func newBatchPoints(precision, database, rp string) (influxdb.BatchPoints, error) {
	return influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Precision:       precision,
		Database:        database,
		RetentionPolicy: rp,
	})
}

// queryDB convenience function to query the database
func (t *Talon) queryDB(cmd string) ([]influxdb.Result, error) {
	response, err := t.client.Query(influxdb.Query{
		Command:  cmd,
		Database: t.getOpts().Influx.Database,
	})
	if err != nil {
		return nil, err
	}
	if response.Error() != nil {
		return nil, response.Error()
	}
	return response.Results, nil
}

func (t *Talon) dropAllSeries() (err error) {
	_, err = t.queryDB("DROP SERIES FROM /.*/")
	return
}

func (t *Talon) getTagValue(measurement string, tagName string) ([]string, error) {
	q := fmt.Sprintf(`SHOW TAG VALUES FROM "%s" WITH KEY = "%s"`, measurement, tagName)
	res, err := t.queryDB(q)
	if err != nil {
		return nil, err
	}

	tvs := []string{}
	if len(res) < 1 || len(res[0].Series) < 1 || len(res[0].Series[0].Values) < 1 {
		return tvs, nil
	}

	for _, vs := range res[0].Series[0].Values {
		if len(vs) < 2 {
			continue
		}
		if v, ok := vs[1].(string); ok {
			tvs = append(tvs, v)
		}
	}

	return tvs, err
}
