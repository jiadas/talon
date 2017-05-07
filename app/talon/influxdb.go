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

func (t *Talon) newBatchPoints() (err error) {
	// make sure we're only creating new batch points when we don't already have them
	if t.batchP != nil {
		return nil
	}
	t.batchP, err = influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  t.database,
		Precision: t.precision,
	})
	if err != nil {
		return err
	}
	return nil
}
