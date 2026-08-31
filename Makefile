PWD = $(shell pwd)
SHELL := /bin/bash
MODULES ?= bkmonitorbeat collector operator transfer unify-query influxdb-proxy ingester offline-data-archive bk-monitor-worker sliwebhook bkm-ksm-exporter
RELEASE_PATH ?= $(PWD)/dist
BUILD_NO ?= 1
COMMIT_ID = $(shell git rev-parse HEAD)
MODULE =
RELEASE ?= false

VERSION_FILE = $(if $(strip $(MODULE)),$(PWD)/pkg/$(MODULE)/VERSION,)
MODULE_VERSION = $(subst v,ee-V, $(strip $(shell [ -n "$(VERSION_FILE)" ] && cat "$(VERSION_FILE)" || true)))
BRANCH ?= $(shell git symbolic-ref --short HEAD)
# VERSION 解析遵循各模块自己的 pkg/<module>/VERSION 模板：
# - 模板以 v 开头（如 v3.9.x）时，默认构建和 tag 解析后的版本都保留/补上 v
# - 模板不以 v 开头（如 0.102.x）时，默认构建和 tag 解析后的版本都不带 v
VERSION_TEMPLATE = $(strip $(shell [ -n "$(VERSION_FILE)" ] && cat "$(VERSION_FILE)" || true))
VERSION_PREFIX = $(shell printf '%s' '$(VERSION_TEMPLATE)' | grep -Eq '^v' && printf 'v' || true)

ifeq ($(RELEASE), true)
	PACKAGE_VERSION = $(MODULE_VERSION)
else
	PACKAGE_VERSION = $(MODULE_VERSION)-$(BRANCH)
endif

# TAG_INPUT 优先取 CI 显式传入的 BUILD_GIT_TAG，其次取 GIT_TAG。
# 只有 tag 形如 release/<module>/a.b.c 或 beta/<module>/a.b.c.d 时，才视为可解析的发布 tag。
TAG_INPUT = $(strip $(if $(BUILD_GIT_TAG),$(BUILD_GIT_TAG),$(GIT_TAG)))
# 用于判断 TAG_INPUT 是否真的是符合规则的 tag，避免把普通分支名/引用误判成 tag。
TAG_PATTERN_MATCH = $(shell tag="$(TAG_INPUT)"; if echo "$$tag" | grep -Eq '^release/[^/]+/[0-9]+\.[0-9]+\.[0-9]+$$|^beta/[^/]+/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$$'; then echo true; else echo false; fi)
# GIT_REF_KIND 判定规则：
# 1. 显式传了 GIT_REF 时，以其值为准（常见为 TAG / BRANCH）；
# 2. 未传 GIT_REF 时，仅当 TAG_INPUT 符合支持的 tag 规则才按 TAG 处理；
# 3. 其余情况统一按 BRANCH 处理，避免误触发 tag 校验。
GIT_REF_KIND = $(shell printf '%s' '$(strip $(if $(GIT_REF),$(GIT_REF),$(if $(filter true,$(TAG_PATTERN_MATCH)),TAG,BRANCH)))' | tr '[:lower:]' '[:upper:]')
MODULE_CHECK_INPUT = $(strip $(if $(MODULE_NAME),$(MODULE_NAME),$(MODULE)))
# 仅 TAG 构建才解析 tag 中的 <module> 和版本号：
# - release/<module>/a.b.c      -> TAG_MODULE=<module>, TAG_VERSION=a.b.c
# - beta/<module>/a.b.c.d       -> TAG_MODULE=<module>, TAG_VERSION=a.b.c.d
# 只要 TAG_INPUT 本身匹配规则，就可提取出基础版本号；
# 其中只有真正的 TAG 构建才会额外执行 tag/module 一致性校验。
TAG_MODULE = $(shell \
	if [ "$(TAG_PATTERN_MATCH)" != "true" ]; then \
		exit 0; \
	fi; \
	tag="$(TAG_INPUT)"; \
	if echo "$$tag" | grep -Eq '^release/[^/]+/[0-9]+\.[0-9]+\.[0-9]+$$'; then \
		echo "$$tag" | cut -d/ -f2; \
	elif echo "$$tag" | grep -Eq '^beta/[^/]+/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$$'; then \
		echo "$$tag" | cut -d/ -f2; \
	fi)
TAG_VERSION = $(shell \
	if [ "$(TAG_PATTERN_MATCH)" != "true" ]; then \
		exit 0; \
	fi; \
	tag="$(TAG_INPUT)"; \
	if echo "$$tag" | grep -Eq '^release/[^/]+/[0-9]+\.[0-9]+\.[0-9]+$$'; then \
		echo "$$tag" | cut -d/ -f3; \
	elif echo "$$tag" | grep -Eq '^beta/[^/]+/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$$'; then \
		echo "$$tag" | cut -d/ -f3; \
	fi)
VERSION_SOURCE = default
ifneq ($(strip $(TAG_VERSION)),)
# 只要存在合法 TAG_INPUT，就先以 tag 里的版本号作为基础版本；
# 是否补 v 前缀仍由各模块自己的 VERSION 模板决定。
BASE_VERSION = $(VERSION_PREFIX)$(TAG_VERSION)
VERSION_SOURCE = tag
else
# 无合法 TAG_INPUT 时，按模块 VERSION 模板把 x 替换为 BUILD_NO 作为基础版本。
BASE_VERSION = $(subst x,$(BUILD_NO),$(VERSION_TEMPLATE))
endif
# 非 TAG 构建：
# - 若基础版本只有 3 段（如 0.102.9 / v3.9.8），则追加第 4 位 .CI_BUILD_NUM；
# - 若基础版本本身已是 4 段（如 beta tag 解析出的 0.102.9.5），则直接保留。
BASE_DOT_COUNT = $(shell printf '%s' '$(BASE_VERSION)' | tr -cd '.' | wc -c | tr -d ' ')
VERSION = $(if $(filter TAG,$(GIT_REF_KIND)),$(BASE_VERSION),$(if $(filter 2,$(BASE_DOT_COUNT)),$(BASE_VERSION).$(CI_BUILD_NUM_VALUE),$(BASE_VERSION)))
DOT_COUNT = $(shell printf '%s' '$(VERSION)' | tr -cd '.' | wc -c | tr -d ' ')
ifeq ($(DOT_COUNT),2)
RELEASE_VALUE = true
else
RELEASE_VALUE = false
endif
GIT_REF_NAME = $(if $(TAG_INPUT),$(TAG_INPUT),$(shell git symbolic-ref --short HEAD 2>/dev/null || git rev-parse --short HEAD))
GIT_REF_TYPE = $(if $(filter TAG,$(GIT_REF_KIND)),tag,branch)
GIT_COMMIT_TIME = $(shell git log -1 --format=%cI $(COMMIT_ID) 2>/dev/null || true)
GIT_COMMITTER_NAME = $(shell git log -1 --format=%cn $(COMMIT_ID) 2>/dev/null || true)
GIT_COMMITTER_EMAIL = $(shell git log -1 --format=%ce $(COMMIT_ID) 2>/dev/null || true)
CI_BUILD_ID_VALUE = $(or $(CI_BUILD_ID),$(BUILD_ID))
CI_BUILD_NUM_VALUE = $(or $(CI_BUILD_NUM),$(BUILD_NO),$(BUILD_NUM))
ifneq ($(strip $(CI_BUILD_PIPELINE_NAME)),)
CI_PIPELINE_VALUE = $(CI_BUILD_PIPELINE_NAME)
else
CI_PIPELINE_VALUE =
endif
GIT_REPO_RAW = $(or $(GIT_REPO),$(shell git config --get remote.origin.url 2>/dev/null || true))
GIT_REPO_VALUE = $(shell printf '%s' '$(GIT_REPO_RAW)' | sed -E 's|^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@]+@|\1|')
TAG = pkg/$(MODULE)/$(if $(filter v%,$(VERSION)),$(VERSION),v$(VERSION))
PIP_PATH ?= $(shell which pip)

.PHONY: all
all: bkmonitorbeat collector operator transfer unify-query influxdb-proxy ingester offline-data-archive bk-monitor-worker sliwebhook bkm-ksm-exporter

.PHONY: .check_module_vars
.check_module_vars:
	@test $${MODULE?Please set environment variable MODULE}

.PHONY: build
build: .check_module_vars
	# 只有 TAG 构建才做 tag 模块名校验；
	# BRANCH/普通构建直接跳过，避免分支名或其他引用被误判为 tag 而失败。
	@if [ "$(GIT_REF_KIND)" = "TAG" ] && [ -n "$(TAG_INPUT)" ] && [ -n "$(MODULE_CHECK_INPUT)" ]; then \
		tag_module="$(TAG_MODULE)"; \
		if [ -z "$$tag_module" ] || ! printf '%s' "$$tag_module" | grep -Fq "$(MODULE_CHECK_INPUT)"; then \
			echo "打包错误：tag号未包含$(MODULE_CHECK_INPUT)信息，示例为：release/\$${module}/a.b.c或beta/\$${module}/a.b.c.d"; \
			exit 1; \
		fi; \
	fi
	mkdir -p $(RELEASE_PATH)/$(MODULE)
	@echo make module: $(MODULE) BUILD_NO: $(BUILD_NO) COMMIT_ID: $(COMMIT_ID)
	@echo version resolve: ref_kind='$(GIT_REF_KIND)' source=$(VERSION_SOURCE) template='$(VERSION_TEMPLATE)' prefix='$(VERSION_PREFIX)' tag_input='$(TAG_INPUT)' tag_match='$(TAG_PATTERN_MATCH)' module_check='$(MODULE_CHECK_INPUT)' tag_module='$(TAG_MODULE)' tag_version='$(TAG_VERSION)' base_version='$(BASE_VERSION)' base_dot_count='$(BASE_DOT_COUNT)' final_version='$(VERSION)' release='$(RELEASE_VALUE)'
	cd $(PWD)/pkg/$(MODULE) && make RELEASE_PATH=$(RELEASE_PATH)/$(MODULE) VERSION=$(VERSION) BUILD_NO=$(BUILD_NO) COMMIT_ID=$(COMMIT_ID) JSON_LIB=$(JSON_LIB) GIT_TAG=$(TAG_INPUT) GIT_COMMIT_TIME='$(GIT_COMMIT_TIME)' GIT_COMMITTER_NAME='$(GIT_COMMITTER_NAME)' CI_BUILD_ID='$(CI_BUILD_ID_VALUE)' CI_BUILD_NUM='$(CI_BUILD_NUM_VALUE)' CI_PIPELINE='$(CI_PIPELINE_VALUE)' RELEASE_VALUE=$(RELEASE_VALUE) build
	@printf '%s\n' \
		'name: $(MODULE)' \
		'version: $(VERSION)' \
		'release: $(RELEASE_VALUE)' \
		'ci_info:' \
		'  ci_build_id: $(CI_BUILD_ID_VALUE)' \
		'  ci_build_num: $(CI_BUILD_NUM_VALUE)' \
		'  ci_pipeline: $(CI_PIPELINE_VALUE)' \
		'  git_repo: $(GIT_REPO_VALUE)' \
		'  git_last_commit:' \
		'    ref_name: $(GIT_REF_NAME)' \
		'    ref_type: $(GIT_REF_TYPE)' \
		'    hash: $(COMMIT_ID)' \
		'    time: $(GIT_COMMIT_TIME)' \
		'    committer: $(GIT_COMMITTER_NAME)' \
		'    committer_email: $(GIT_COMMITTER_EMAIL)' \
		> $(RELEASE_PATH)/$(MODULE)/ci_git.yaml
	@if ls $(RELEASE_PATH)/$(MODULE)/*.tgz >/dev/null 2>&1; then \
		inner_dir="$(MODULE)"; \
		if [ "$(MODULE)" = "collector" ]; then inner_dir="bk-collector"; fi; \
		for archive in $(RELEASE_PATH)/$(MODULE)/*.tgz; do \
			tmp_dir="$$(mktemp -d)"; \
			tar -zxf "$$archive" -C "$$tmp_dir"; \
			found=0; \
			while IFS= read -r -d '' target_dir; do \
				cp $(RELEASE_PATH)/$(MODULE)/ci_git.yaml "$$target_dir/ci_git.yaml"; \
				found=1; \
			done < <(find "$$tmp_dir" -type d -name "$$inner_dir" -print0); \
			if [ "$$found" -eq 1 ]; then \
				tar -zcf "$$archive" -C "$$tmp_dir" .; \
				echo "inject ci_git.yaml into $$archive"; \
			fi; \
			rm -rf "$$tmp_dir"; \
		done; \
	fi

.PHONY: bkmonitorbeat
bkmonitorbeat:
	$(MAKE) MODULE=bkmonitorbeat build

.PHONY: collector
collector:
	$(MAKE) MODULE=collector build

.PHONY: operator
operator:
	$(MAKE) MODULE=operator build

.PHONY: sliwebhook
sliwebhook:
	$(MAKE) MODULE=sliwebhook build

.PHONY: bkm-ksm-exporter
bkm-ksm-exporter:
	$(MAKE) MODULE=bkm-ksm-exporter build

.PHONY: transfer
transfer:
	$(MAKE) MODULE=transfer build

.PHONY: unify-query
unify-query:
	$(MAKE) MODULE=unify-query build

.PHONY: influxdb-proxy
influxdb-proxy:
	$(MAKE) MODULE=influxdb-proxy build

.PHONY: ingester
ingester:
	$(MAKE) MODULE=ingester build

.PHONY: offline-data-archive
offline-data-archive:
	$(MAKE) MODULE=offline-data-archive build

.PHONY: bk-monitor-worker
bk-monitor-worker:
	$(MAKE) MODULE=bk-monitor-worker build

.PHONY: version
version: .check_module_vars
	@echo $(VERSION)

.PHONY: package_version
package_version:
	@echo $(PACKAGE_VERSION)

.PHONY: tag
tag: .check_module_vars
	cd $(PWD)/pkg/$(MODULE) || exit 1
	@echo tag: $(TAG)
	git tag $(TAG)
	git push --tags

.PHONY: lint
lint:
	cd $(PWD)/pkg/$(MODULE) && make lint

.PHONY: test
test:
	cd $(PWD)/pkg/$(MODULE) && make test

.PHONY: fmt
fmt:
	cd $(PWD)/pkg/$(MODULE) && go fmt ./...

.PHONY: debug
debug: .check_module_vars
	mkdir -p $(RELEASE_PATH)/$(MODULE)
	@echo make module: $(MODULE) BUILD_NO: $(BUILD_NO) COMMIT_ID: $(COMMIT_ID)
	cd $(PWD)/pkg/$(MODULE) && make RELEASE_PATH=$(RELEASE_PATH)/$(MODULE) VERSION=$(VERSION) BUILD_NO=$(BUILD_NO) COMMIT_ID=$(COMMIT_ID) JSON_LIB=$(JSON_LIB) debug

.PHONY: pre-commit
pre-commit:
	pre-commit run -a

.check_pip_vars:
	@test $(PIP_PATH)||(echo 'pip command or PIP_PATH not found'&&exit 1)

.PHONY: pre-commit-install
pre-commit-install: .check_pip_vars
	$(PIP_PATH) install -r scripts/requirements.txt
	go install github.com/google/addlicense@latest
	go install github.com/incu6us/goimports-reviser/v3@latest
	pre-commit clean
	pre-commit install

.PHONY: addlicense
addlicense:
	find ./ -type f \( -iname \*.go -o -iname \*.py -iname \*.sh \)|xargs addlicense -v -f scripts/license.txt -ignore vendor/*

.PHONY: imports
imports: addlicense
	goimports-reviser -project-name "github.com/TencentBlueKing/bkmonitor-datalink/pkg" ./...
