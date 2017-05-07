package main

import (
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/jiadas/talon/app/talon"
	"github.com/judwhite/go-svc/svc"
)

type program struct {
	talon *talon.Talon
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		log.WithError(err).Fatal("failed to start")
	}
}

func (p *program) Init(env svc.Environment) error {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})
	return nil
}

func (p *program) Start() error {
	t := talon.New()

	err := t.LoadMetadata()
	if err != nil {
		log.WithError(err).Fatal("failed to load metadata")
	}

	// TODO handle error
	t.Capture()

	p.talon = t
	return nil
}

func (p *program) Stop() error {
	if p.talon != nil {
		p.talon.Exit()
	}
	return nil
}
