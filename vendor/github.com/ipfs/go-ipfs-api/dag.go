package shell

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	files "github.com/ipfs/boxo/files"
	"github.com/ipfs/go-ipfs-api/options"
)

type DagPutOutput struct {
	Cid struct {
		Target string `json:"/"`
	}
}

type DagImportRoot struct {
	Root struct {
		Cid struct {
			Value string `json:"/"`
		}
	}
	Stats *DagImportStats `json:"Stats,omitempty"`
}

type DagImportStats struct {
	BlockBytesCount uint64
	BlockCount      uint64
}

type DagImportOutput struct {
	Roots []DagImportRoot
	Stats *DagImportStats
}

func (s *Shell) DagGet(ref string, out interface{}) error {
	return s.Request("dag/get", ref).Exec(context.Background(), out)
}

func (s *Shell) DagPut(data interface{}, inputCodec, storeCodec string) (string, error) {
	return s.DagPutWithOpts(data, options.Dag.InputCodec(inputCodec), options.Dag.StoreCodec(storeCodec))
}

func (s *Shell) DagPutWithOpts(data interface{}, opts ...options.DagPutOption) (string, error) {
	cfg, err := options.DagPutOptions(opts...)
	if err != nil {
		return "", err
	}

	fileReader, err := s.dagToFilesReader(data)
	if err != nil {
		return "", err
	}

	var out DagPutOutput

	return out.Cid.Target, s.
		Request("dag/put").
		Option("input-codec", cfg.InputCodec).
		Option("store-codec", cfg.StoreCodec).
		Option("pin", cfg.Pin).
		Option("hash", cfg.Hash).
		Body(fileReader).
		Exec(context.Background(), &out)
}

// DagImport imports the contents of .car files (with default parameters)
func (s *Shell) DagImport(data interface{}, silent, stats bool) (*DagImportOutput, error) {
	return s.DagImportWithOpts(data, options.Dag.Silent(silent), options.Dag.Stats(stats))
}

// DagImportWithOpts imports the contents of .car files
func (s *Shell) DagImportWithOpts(data interface{}, opts ...options.DagImportOption) (*DagImportOutput, error) {
	cfg, err := options.DagImportOptions(opts...)
	if err != nil {
		return nil, err
	}

	fileReader, err := s.dagToFilesReader(data)
	if err != nil {
		return nil, err
	}

	res, err := s.Request("dag/import").
		Option("pin-roots", cfg.PinRoots).
		Option("silent", cfg.Silent).
		Option("stats", cfg.Stats).
		Option("allow-big-block", cfg.AllowBigBlock).
		Body(fileReader).
		Send(context.Background())
	if err != nil {
		return nil, err
	}
	defer res.Close()

	if res.Error != nil {
		return nil, res.Error
	}

	if cfg.Silent {
		return nil, nil
	}

	out := DagImportOutput{
		Roots: []DagImportRoot{},
	}

	dec := json.NewDecoder(res.Output)

	for {
		var root DagImportRoot
		err := dec.Decode(&root)
		if err == io.EOF {
			break
		}

		if root.Stats != nil {
			out.Stats = root.Stats

			break
		}

		out.Roots = append(out.Roots, root)
	}

	return &out, err
}

func (s *Shell) dagToFilesReader(data interface{}) (*files.MultiFileReader, error) {
	var r io.Reader
	switch data := data.(type) {
	case *files.MultiFileReader:
		return data, nil
	case string:
		r = strings.NewReader(data)
	case []byte:
		r = bytes.NewReader(data)
	case io.Reader:
		r = data
	default:
		return nil, fmt.Errorf("values of type %T cannot be handled as DAG input", data)
	}

	fr := files.NewReaderFile(r)
	slf := files.NewSliceDirectory([]files.DirEntry{files.FileEntry("", fr)})
	return s.newMultiFileReader(slf)
}
