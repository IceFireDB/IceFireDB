PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)

all: $(GO)

%.pb.go: %.proto
	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative $<

clean:
	rm -f *.pb.go
