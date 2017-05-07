package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/jiadas/talon/app/talon"
)

func main() {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})

	t := talon.New()
	t.Capture()
}
