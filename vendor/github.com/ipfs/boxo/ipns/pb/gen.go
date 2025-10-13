// These commands work around namespace conflicts that occur when multiple
// repositories depend on .proto files with generic filenames. Due to the way
// protobuf registers files (e.g., foo.proto or pb/foo.proto), naming
// collisions can occur when the same filename is used across different
// packages.
//
// The only way to generate a *.pb.go file that includes the full package path
// (e.g., github.com/org/repo/pb/foo.proto) is to place the .proto file in a
// directory structure that mirrors its package path.
//
// References:
//   - https://protobuf.dev/reference/go/faq#namespace-conflict
//   - https://github.com/golang/protobuf/issues/1122#issuecomment-2045945265
//
//go:generate mkdir -p github.com/ipfs/boxo/ipns/pb
//go:generate ln -f record.proto github.com/ipfs/boxo/ipns/pb/
//go:generate protoc --go_out=. github.com/ipfs/boxo/ipns/pb/record.proto
//go:generate mv -f github.com/ipfs/boxo/ipns/pb/record.pb.go .
//go:generate rm -rf github.com
package pb
