# IPFS library helpers are wrappers

This submodule contains wrapper implementations of IPFS-stack interfaces to
introduce blocking-functionality as provided by NOpfs.

This submodule has its own `go.mod` and `go.sum` files and depend on the
version of Boxo that we are building it for.

This submodule is tagged in the form `ipfs/v<BOXO_VERSION>.<RELEASE_NUMBER>`
where the Boxo version corresponds to the Boxo release that the code targets
and the release suffix to the release number for that version (in case of
multiple).

