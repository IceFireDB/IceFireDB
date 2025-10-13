package options

// DagImportSettings is a set of DagImport options.
type DagImportSettings struct {
	PinRoots      bool
	Silent        bool
	Stats         bool
	AllowBigBlock bool
}

// DagImportOption is a single DagImport option.
type DagImportOption func(opts *DagImportSettings) error

// DagImportOptions applies the given options to a DagImportSettings instance.
func DagImportOptions(opts ...DagImportOption) (*DagImportSettings, error) {
	options := &DagImportSettings{
		PinRoots:      true,
		Silent:        false,
		Stats:         false,
		AllowBigBlock: false,
	}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, err
		}
	}

	return options, nil
}

// PinRoots is an option for Dag.Import which specifies whether to
// pin the optional roots listed in the .car headers after importing.
// Default is true.
func (dagOpts) PinRoots(pinRoots bool) DagImportOption {
	return func(opts *DagImportSettings) error {
		opts.PinRoots = pinRoots
		return nil
	}
}

// Silent is an option for Dag.Import which specifies whether to
// return any output or not.
// Default is false.
func (dagOpts) Silent(silent bool) DagImportOption {
	return func(opts *DagImportSettings) error {
		opts.Silent = silent
		return nil
	}
}

// Stats is an option for Dag.Import which specifies whether to
// return stats about the import operation.
// Default is false.
func (dagOpts) Stats(stats bool) DagImportOption {
	return func(opts *DagImportSettings) error {
		opts.Stats = stats
		return nil
	}
}

// AllowBigBlock is an option for Dag.Import which disables block size check
// and allow creation of blocks bigger than 1MiB.
// WARNING: such blocks won't be transferable over the standard bitswap.
// Default is false.
func (dagOpts) AllowBigBlock(allowBigBlock bool) DagImportOption {
	return func(opts *DagImportSettings) error {
		opts.AllowBigBlock = allowBigBlock
		return nil
	}
}
