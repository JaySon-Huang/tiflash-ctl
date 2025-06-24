$(info    VERSION is $(VERSION))

GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build
GOTEST   := CGO_ENABLED=0 $(GO) test -p 2

PACKAGES  := $$(go list ./...)
FILES     := $$(find . -name "*.go")

.PHONY: default test

default:
	$(GOBUILD) -o bin/tiflash-ctl

release_all:
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o bin/tiflash-ctl-linux-amd64
	GOOS=linux GOARCH=arm64 $(GOBUILD) -o bin/tiflash-ctl-linux-arm64

	@# Build failures on macOS due to gosigar package.
	@#github.com/elastic/gosigar
	@#../../ra_common/go_home/go1.23.5/pkg/mod/github.com/elastic/gosigar@v0.14.2/concrete_sigar.go:20:12: cpuUsage.Get undefined (type Cpu has no field or method Get)
	@#GOOS=darwin GOARCH=amd64 $(GOBUILD) -o bin/tiflash-ctl-darwin-amd64
	@#GOOS=darwin GOARCH=arm64 $(GOBUILD) -o bin/tiflash-ctl-darwin-arm64

xz_release: release_all
	@echo "Creating xz compressed files..."
	@mkdir -p bin/xz
	tar cJf bin/xz/tiflash-ctl-linux-amd64.tar.xz -C bin tiflash-ctl-linux-amd64
	tar cJf bin/xz/tiflash-ctl-linux-arm64.tar.xz -C bin tiflash-ctl-linux-arm64
	@# Uncomment the following lines if macOS builds are enabled.
	@# tar cJf bin/xz/tiflash-ctl-darwin-amd64.tar.xz -C bin tiflash-ctl-darwin-amd64
	@# tar cJf bin/xz/tiflash-ctl-darwin-arm64.tar.xz -C bin tiflash-ctl-darwin-arm64

test:
	$(GOTEST) -timeout 30s ./...
