package main

import (
	"math/rand"
	"syscall"
	"time"

	"github.com/jiadas/talon/app/config"
	"github.com/jiadas/talon/app/shared/logger"
	"github.com/jiadas/talon/app/talon"
	"github.com/judwhite/go-svc/svc"
	log "github.com/sirupsen/logrus"
)

type program struct {
	opts  *config.Options
	talon *talon.Talon
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		log.WithError(err).Fatal("failed to start")
	}
}

func (p *program) Init(env svc.Environment) error {
	p.opts = config.NewOptions()
	p.opts.SetByFlags()

	logger.Init(p.opts)

	rand.Seed(time.Now().UnixNano())

	return nil
}

func (p *program) Start() error {
	t := talon.New(p.opts)

	err := t.LoadMetadata()
	if err != nil {
		log.WithError(err).Fatal("failed to load metadata")
	}

	t.Capture()

	p.talon = t

	// 加版本信息，每次修改代码的时候修改版本信息，确保最新的修改被正确部署
	log.WithField("version", "v9").Info("talon started")

	return nil
}

func (p *program) Stop() error {
	if p.talon != nil {
		p.talon.Exit()
	}
	return nil
}
