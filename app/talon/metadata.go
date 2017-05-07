package talon

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

type meta struct {
	Shards []struct {
		ID     int    `json:"id"`
		Cursor string `json:"cursor"`
	}
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

func (t *Talon) LoadMetadata() error {
	fn := path.Join(t.dataPath, "talon.dat")
	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // fresh start
	}

	var m meta
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	for _, s := range m.Shards {
		t.cursorMap[s.ID] = s.Cursor
	}
	return nil
}
