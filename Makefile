.PHONY: all build build-linux test clean

BINARY_NAME=kafka-drainer

all: test build

build:
	go build -ldflags="-s -w" -o $(BINARY_NAME) .

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o $(BINARY_NAME)-linux-amd64 .

test:
	go test -v ./...

clean:
	rm -f $(BINARY_NAME) $(BINARY_NAME)-linux-amd64
