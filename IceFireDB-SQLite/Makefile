PROG=bin/IceFireDB-SQLite

SRCS=./main.go

CFLAGS = -ldflags "-s -w "

build:
	if [ ! -d "./bin/" ]; then \
		mkdir bin; \
	fi
	go build $(CFLAGS) -o $(PROG) $(SRCS)

race:
	if [ ! -d "./bin/" ]; then \
    	mkdir bin; \
    fi
	go build $(CFLAGS) -race -o $(PROG) $(SRCS)

clean:
	rm -rf ./bin

run:
	go run --race main.go -c config/config.yaml