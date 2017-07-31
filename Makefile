TARGET   ?= $(shell basename `git rev-parse --show-toplevel`)
VERSION  ?= $(shell git describe --tags --always )
BRANCH   ?= $(shell git rev-parse --abbrev-ref HEAD)
REVISION ?= $(shell git rev-parse HEAD)
SHORTREV ?= $(shell git rev-parse --short HEAD)
LD_FLAGS ?= -s \
	-X github.com/Nordstrom/kandi/version.Name=$(TARGET) \
	-X github.com/Nordstrom/kandi/version.Revision=$(REVISION) \
	-X github.com/Nordstrom/kandi/version.Branch=$(BRANCH) \
	-X github.com/Nordstrom/kandi/version.Version=$(VERSION)

TESTS ?= $(shell go list ./... | grep -v /vendor/)

default: test build

test:
	go test -v -cover -run=$(RUN) $(TESTS)

build: clean
	@go build -v \
		-ldflags "$(LD_FLAGS)+local_changes" \
		-o bin/$(TARGET) .

release: test clean
	@CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build \
		-a -tags netgo \
		-a -installsuffix cgo \
		-ldflags "$(LD_FLAGS)" \
		-o bin/release/$(TARGET) .

docker/build: release
	@docker build -t kandi .

docker/push: docker/build
	@docker tag kandi quay.io/nordstrom/kandi:$(SHORTREV)
	@docker push quay.io/nordstrom/kandi:$(SHORTREV)

clean:
	@rm -rf bin/
