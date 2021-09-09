GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build

PACKAGES  := $$(go list ./...)
FILES     := $$(find . -name "*.go")

.PHONY: default

default:
	$(GOBUILD) -o bin/tiflash-ctl
