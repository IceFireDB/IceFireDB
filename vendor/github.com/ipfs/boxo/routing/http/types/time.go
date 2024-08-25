package types

import (
	"encoding/json"
	"time"

	"github.com/ipfs/boxo/routing/http/internal/drjson"
)

type Time struct{ time.Time }

func (t *Time) MarshalJSON() ([]byte, error) {
	return drjson.MarshalJSONBytes(t.Time.UnixMilli())
}

func (t *Time) UnmarshalJSON(b []byte) error {
	var timestamp int64
	err := json.Unmarshal(b, &timestamp)
	if err != nil {
		return err
	}
	t.Time = time.UnixMilli(timestamp)
	return nil
}

type Duration struct{ time.Duration }

func (d *Duration) MarshalJSON() ([]byte, error) { return drjson.MarshalJSONBytes(d.Duration) }
func (d *Duration) UnmarshalJSON(b []byte) error {
	var dur int64
	err := json.Unmarshal(b, &dur)
	if err != nil {
		return err
	}
	d.Duration = time.Duration(dur)
	return nil
}
