RELEASE_PATH ?= bin
BUILD_NO ?= 0
NAME = bkmonitorbeat
VERSION = ''
COMMIT_ID = ''

.PHONY: lint
lint:
	gofumpt -w .
	goimports-reviser -project-name "github.com/TencentBlueKing/bkmonitor-datalink/pkg" ./...

.PHONY: build
build:
	env VERSION=$(VERSION) RELEASE_PATH=$(RELEASE_PATH) GIT_TAG=$(GIT_TAG) GIT_COMMIT_TIME='$(GIT_COMMIT_TIME)' GIT_COMMITTER_NAME='$(GIT_COMMITTER_NAME)' CI_BUILD_ID='$(CI_BUILD_ID)' CI_BUILD_NUM='$(CI_BUILD_NUM)' CI_PIPELINE='$(CI_PIPELINE)' RELEASE_VALUE=$(RELEASE_VALUE) ./script/gse_package.sh

.PHONY: test
test:
	go test ./...
