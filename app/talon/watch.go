package talon

import (
	"context"

	"encoding/json"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/jiadas/talon/app/config"
	"github.com/jiadas/talon/app/shared/etcd"
)

func (t *Talon) watchRemoteConfig() {
	rch := etcd.Client().Watch(context.Background(), config.TopicsKey)
	for {
		wresp := <-rch
		for _, ev := range wresp.Events {
			if ev.Type == mvccpb.PUT && string(ev.Kv.Key) == config.TopicsKey {
				var topics config.Topics
				json.Unmarshal(ev.Kv.Value, &topics)

				opts := *t.getOpts()
				opts.Topics = topics
				t.swapOpts(&opts)
			}
		}
	}
}
