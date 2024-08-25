package provider

import (
	"context"

	"github.com/ipfs/go-cid"
)

type noopProvider struct{}

var _ System = (*noopProvider)(nil)

// NewNoopProvider creates a ProviderSystem that does nothing.
func NewNoopProvider() System {
	return &noopProvider{}
}

func (op *noopProvider) Close() error {
	return nil
}

func (op *noopProvider) Provide(cid.Cid) error {
	return nil
}

func (op *noopProvider) Reprovide(context.Context) error {
	return nil
}

func (op *noopProvider) Stat() (ReproviderStats, error) {
	return ReproviderStats{}, nil
}
