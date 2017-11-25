package logger

import (
	"time"

	"github.com/NYTimes/logrotate"
	"github.com/jiadas/talon/app/config"
	log "github.com/sirupsen/logrus"
)

func Init(opts *config.Options) {
	if !opts.IsOnlineEnvironment() {
		log.SetFormatter(&log.TextFormatter{
			ForceColors:   true,
			FullTimestamp: true,
		})
		return
	}

	log.SetFormatter(&log.JSONFormatter{TimestampFormat: time.RFC3339Nano})

	dir := opts.LogDir
	if dir != "" {
		logpath := dir + "/go_talon.log_json.logrus"

		logfile, err := logrotate.NewFile(logpath)
		if err != nil {
			log.Error(err)
			return
		}
		log.SetOutput(logfile)
	}
}
