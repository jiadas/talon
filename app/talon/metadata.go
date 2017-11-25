package talon

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"

	log "github.com/sirupsen/logrus"
)

type meta struct {
	Shards []struct {
		ID     int    `json:"id"`
		Cursor string `json:"cursor"`
	} `json:"shards"`
}

func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

func (t *Talon) LoadMetadata() error {
	fileName := path.Join(t.getOpts().DataPath, "talon.dat")
	data, err := readOrEmpty(fileName)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // fresh start
	}

	var m meta
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fileName, err)
	}

	for _, s := range m.Shards {
		t.cursorMap[s.ID] = s.Cursor
	}
	return nil
}

// todo 每隔一段时间就PersistMetadata，因为程序突然挂了不会执行写文件，被supervisor重新拉起就还是加载的很早之前的talon.dat

func (t *Talon) PersistMetadata() error {
	fileName := path.Join(t.getOpts().DataPath, "talon.dat")

	log.WithField("dataPaht", fileName).Info("persisting cursor metadata")

	js := make(map[string]interface{})
	shards := []interface{}{}
	for id, cursor := range t.cursorMap {
		shardData := make(map[string]interface{})
		shardData["id"] = id
		shardData["cursor"] = cursor
		shards = append(shards, shardData)
	}
	js["shards"] = shards

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}
