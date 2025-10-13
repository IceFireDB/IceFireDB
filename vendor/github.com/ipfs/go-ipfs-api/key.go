package shell

import (
	"context"
	"io"

	files "github.com/ipfs/boxo/files"
)

type Key struct {
	Id   string
	Name string
}

type KeyRenameObject struct {
	Id        string
	Now       string
	Overwrite bool
	Was       string
}

type keyListOutput struct {
	Keys []*Key
}

type KeyOpt func(*RequestBuilder) error
type keyGen struct{}

var KeyGen keyGen

func (keyGen) Type(alg string) KeyOpt {
	return func(rb *RequestBuilder) error {
		rb.Option("type", alg)
		return nil
	}
}

func (keyGen) Size(size int) KeyOpt {
	return func(rb *RequestBuilder) error {
		rb.Option("size", size)
		return nil
	}
}

// KeyGen Create a new keypair
func (s *Shell) KeyGen(ctx context.Context, name string, options ...KeyOpt) (*Key, error) {
	rb := s.Request("key/gen", name)
	for _, opt := range options {
		if err := opt(rb); err != nil {
			return nil, err
		}
	}

	var out Key
	if err := rb.Exec(ctx, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// KeyList List all local keypairs
func (s *Shell) KeyList(ctx context.Context) ([]*Key, error) {
	var out keyListOutput
	if err := s.Request("key/list").Exec(ctx, &out); err != nil {
		return nil, err
	}
	return out.Keys, nil
}

// KeyRename Rename a keypair
func (s *Shell) KeyRename(ctx context.Context, old string, new string, force bool) (*KeyRenameObject, error) {
	var out KeyRenameObject
	if err := s.Request("key/rename", old, new).
		Option("force", force).
		Exec(ctx, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// KeyRm remove a keypair
func (s *Shell) KeyRm(ctx context.Context, name string) ([]*Key, error) {
	var out keyListOutput
	if err := s.Request("key/rm", name).
		Exec(ctx, &out); err != nil {
		return nil, err
	}
	return out.Keys, nil
}

type KeyImportOpt func(*RequestBuilder) error
type keyImportOpt struct{}

var KeyImportGen keyImportOpt

func (keyImportOpt) IpnsBase(enc string) KeyImportOpt {
	return func(rb *RequestBuilder) error {
		rb.Option("ipns-base", enc)
		return nil
	}
}

func (keyImportOpt) Format(format string) KeyImportOpt {
	return func(rb *RequestBuilder) error {
		rb.Option("format", format)
		return nil
	}
}

func (keyImportOpt) AllowAnyKeyType(allow bool) KeyImportOpt {
	return func(rb *RequestBuilder) error {
		rb.Option("allow-any-key-type", allow)
		return nil
	}
}

// KeyImport imports key as file.
func (s *Shell) KeyImport(ctx context.Context, name string, key io.Reader, options ...KeyImportOpt) error {
	fr := files.NewReaderFile(key)
	slf := files.NewSliceDirectory([]files.DirEntry{files.FileEntry("", fr)})
	fileReader, err := s.newMultiFileReader(slf)
	if err != nil {
		return err
	}

	rb := s.Request("key/import", name)
	for _, opt := range options {
		if err := opt(rb); err != nil {
			return err
		}
	}

	return rb.Body(fileReader).Exec(ctx, nil)
}
