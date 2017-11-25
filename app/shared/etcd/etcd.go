package etcd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
)

var client *clientv3.Client
var once sync.Once

func Client() *clientv3.Client {
	once.Do(initClient)
	return client
}

func initClient() {
	v, ok := os.LookupEnv("ETCD_ADDR")
	if !ok {
		log.Fatal("not found ETCD_ADDR")
	}
	endpoints := strings.Split(strings.TrimSpace(v), ";")
	var err error
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		log.WithError(err).Fatal("failed to init etcd client")
	}
}

// Get returns the value identified by key
func Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := Client().Get(ctx, key)
	cancel()
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("non-existent key:%s", key)
	}

	return string(resp.Kvs[0].Value), nil
}
