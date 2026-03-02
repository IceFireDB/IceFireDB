package cmds

// HTTPHelpText contains documentation specific to HTTP API responses.
// This is used by tools like http-api-docs to generate API reference
// for HTTP endpoints (e.g., /api/v0/* in Kubo).
type HTTPHelpText struct {
	// ResponseContentType documents the Content-Type header of successful responses.
	// This can be a simple MIME type like "application/vnd.ipld.car" or include
	// additional notes in free-form text, e.g., "application/x-tar, or
	// application/gzip when compress=true".
	// If empty, documentation tools infer from the command's Type field or default to text/plain.
	ResponseContentType string

	// Description provides additional HTTP-specific description text.
	// This is rendered after HelpText.Tagline in HTTP API documentation.
	// Use this to document HTTP-specific behavior, caveats, or notes that
	// don't apply to CLI usage. If empty, only Tagline is shown.
	Description string
}

// HelpText is a set of strings used to generate command help text. The help
// text follows formats similar to man pages, but not exactly the same.
type HelpText struct {
	// required
	Tagline               string            // used in <cmd usage>
	ShortDescription      string            // used in DESCRIPTION
	SynopsisOptionsValues map[string]string // mappings for synopsis generator

	// optional - whole section overrides
	Usage           string // overrides USAGE section
	LongDescription string // overrides DESCRIPTION section
	Options         string // overrides OPTIONS section
	Arguments       string // overrides ARGUMENTS section
	Subcommands     string // overrides SUBCOMMANDS section
	Synopsis        string // overrides SYNOPSIS field

	// HTTP contains documentation specific to the HTTP API.
	// When nil, defaults are derived from other HelpText fields.
	HTTP *HTTPHelpText
}
