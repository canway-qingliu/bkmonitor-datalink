#!/bin/bash
# Tencent is pleased to support the open source community by making
# 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
# Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
# Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://opensource.org/licenses/MIT
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

set -e

# shellcheck disable=SC2006
# shellcheck disable=SC2086

MODULE=bk-collector
TEST_COVERAGE_THRESHOLD=75

GIT_REF_NAME="HEAD"
GIT_REF_TYPE="commit"
GIT_COMMIT_HASH=""
GIT_COMMIT_TIME=""
GIT_COMMITTER_NAME=""
GIT_COMMITTER_EMAIL=""
GIT_TAG=""

CI_BUILD_ID_VALUE=""
CI_BUILD_NUM_VALUE=""
CI_PIPELINE_VALUE=""
GIT_REPO_VALUE=""
RELEASE_VALUE="false"

function sanitize_git_repo_url() {
  local url="$1"

  # Remove credential segment in URLs like https://user:token@host/path
  if printf '%s' "${url}" | grep -Eq '^[a-zA-Z][a-zA-Z0-9+.-]*://[^/@]+@'; then
    printf '%s' "${url}" | sed -E 's#^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@]+@#\1#'
  else
    printf '%s' "${url}"
  fi
}

function collect_git_ci_metadata() {
  local ref_path=""

  if [ -n "${NAME:-}" ]; then
    if git show-ref --verify --quiet "refs/tags/${NAME}"; then
      ref_path="refs/tags/${NAME}"
      GIT_REF_NAME="${NAME}"
      GIT_REF_TYPE="tag"
    elif git show-ref --verify --quiet "refs/heads/${NAME}"; then
      ref_path="refs/heads/${NAME}"
      GIT_REF_NAME="${NAME}"
      GIT_REF_TYPE="branch"
    elif git show-ref --verify --quiet "refs/remotes/origin/${NAME}"; then
      ref_path="refs/remotes/origin/${NAME}"
      GIT_REF_NAME="${NAME}"
      GIT_REF_TYPE="branch"
    fi
  fi

  if [ -z "${ref_path}" ]; then
    ref_path="HEAD"
  fi

  GIT_COMMIT_HASH="$(git rev-parse "${ref_path}^{commit}" 2>/dev/null || true)"
  GIT_COMMIT_TIME="$(git log -1 --format=%cI "${ref_path}" 2>/dev/null || true)"
  GIT_COMMITTER_NAME="$(git log -1 --format=%cn "${ref_path}" 2>/dev/null || true)"
  GIT_COMMITTER_EMAIL="$(git log -1 --format=%ce "${ref_path}" 2>/dev/null || true)"

  # prefer explicitly passed tag from CI, then infer from ref/commit
  if [ -n "${GIT_TAG:-}" ]; then
    GIT_TAG="${GIT_TAG}"
  elif [ -n "${CI_COMMIT_TAG:-}" ]; then
    GIT_TAG="${CI_COMMIT_TAG}"
  elif [ "${GIT_REF_TYPE}" = "tag" ]; then
    GIT_TAG="${GIT_REF_NAME}"
  else
    GIT_TAG="$(git tag --points-at "${GIT_COMMIT_HASH}" 2>/dev/null | head -n 1)"
  fi

  CI_BUILD_ID_VALUE="${CI_BUILD_ID:-${BUILD_ID:-}}"
  CI_BUILD_NUM_VALUE="${CI_BUILD_NUM:-${BUILD_NO:-${BUILD_NUM:-}}}"
  if [ -n "${CI_BUILD_PIPELINE_NAME:-}" ]; then
    CI_PIPELINE_VALUE="${CI_BUILD_PIPELINE_NAME}"
  else
    CI_PIPELINE_VALUE="${CI_PIPELINE:-${CI_PIPELINE_NAME:-${PIPELINE_NAME:-}}/${CI_PIPELINE_VERSION:-${PIPELINE_VERSION:-}}/${CI_WORKSPACE:-${WORKSPACE:-}}}"
  fi
  GIT_REPO_VALUE="$(sanitize_git_repo_url "${GIT_REPO:-$(git config --get remote.origin.url 2>/dev/null || true)}")"
}

function calc_release_value() {
  local version="$1"
  local dot_count

  dot_count="$(printf '%s' "${version}" | tr -cd '.' | wc -c | tr -d ' ')"
  if [ "${dot_count}" = "2" ]; then
    RELEASE_VALUE="true"
  else
    RELEASE_VALUE="false"
  fi
}

function write_ci_git_yaml() {
  local version="$1"
  local dir="$2"

  cat > "${dir}/ci_git.yaml" <<EOF
name: ${MODULE}
version: ${version}
release: ${RELEASE_VALUE}
ci_info:
  ci_build_id: ${CI_BUILD_ID_VALUE}
  ci_build_num: ${CI_BUILD_NUM_VALUE}
  ci_pipeline: ${CI_PIPELINE_VALUE}
  git_repo: ${GIT_REPO_VALUE}
  git_last_commit:
    ref_name: ${GIT_REF_NAME}
    ref_type: ${GIT_REF_TYPE}
    hash: ${GIT_COMMIT_HASH}
    time: ${GIT_COMMIT_TIME}
    committer: ${GIT_COMMITTER_NAME}
    committer_email: ${GIT_COMMITTER_EMAIL}
EOF
}

function unittest() {
  go test ./... -coverprofile coverage.tmp -covermode count
  cat coverage.tmp | grep -v '/gen/' | grep -v '_gen.go' | grep -v '/pb/' > coverage.out
  go tool cover -func coverage.out
  echo "Quality Gate: checking test coverage is above threshold ..."
  echo "Threshold             : $TEST_COVERAGE_THRESHOLD%"
  totalCoverage=$(go tool cover -func=coverage.out | grep total | grep -Eo '[0-9]+\.[0-9]+')
  echo "Current test coverage : $totalCoverage %"
  if (($(echo "$totalCoverage $TEST_COVERAGE_THRESHOLD" | awk '{print ($1 > $2)}'))); then
    echo "OK"
  else
    echo "Current test coverage is below threshold. Please add more unit tests or adjust threshold to a lower value."
    echo "FAIL"
    exit 1
  fi
}

function package() {
  # 变量声明
  local goos=${1:-'linux'}
  local goarch=${2:-'amd64'}
  local arch=${3:-'x86_64'}
  local version=${4:-'v0.0.0'}
  local dist=${5:-'./dist'}

  local dir=${dist}/plugins_${goos}_${arch}/${MODULE}
  local build_time

  # 清空并新建文件夹
  [ -e ${dir} ] && rm -rf ${dir}
  mkdir -p ${dir}/{etc,bin}

  collect_git_ci_metadata
  calc_release_value "${version}"
  build_time="$(date -u '+%Y-%m-%d_%I:%M:%S%p')"

  # 构建二进制
  go mod tidy
  JSON_LIB=${JSON_LIB:-jsonsonic}
  GOOS=${goos} GOARCH=${goarch} \
    go build -tags "${JSON_LIB}" -ldflags " \
  	-s -w \
  	-X main.version=${version} \
  	-X main.buildTime=${build_time} \
  	-X main.gitHash=${GIT_COMMIT_HASH} \
  	-X 'main.tag=${GIT_TAG}' \
  	-X 'main.committer=${GIT_COMMITTER_NAME}' \
  	-X 'main.commitTime=${GIT_COMMIT_TIME}' \
      -X 'main.buildId=${CI_BUILD_ID_VALUE}' \
  	-X 'main.buildNo=${CI_BUILD_NUM_VALUE}' \
   	-X 'main.release=${RELEASE_VALUE}'" \
    -o ${dir}/bin/${MODULE} ./cmd/collector

  # 生成 ci_git.yaml，随打包目录一起发布
  write_ci_git_yaml "${version}" "${dir}"

  # 复制配置
  cp -R ./support-files/templates/${goos}/${arch}/project.yaml ${dir}/project.yaml
  if sed --version >/dev/null 2>&1; then
    sed -i "/^version:/s/.*/version: ${version}/g" "${dir}/project.yaml"
  else
    sed -i '' "/^version:/s/.*/version: ${version}/g" "${dir}/project.yaml"
  fi
  cp -R ./support-files/templates/${goos}/${arch}/etc ${dir}
}

function sidecar() {
    local version=${1:-'v0.0.0'}
    local dist=${2:-'./dist'}
    local build_time

    collect_git_ci_metadata
    calc_release_value "${version}"
    build_time="$(date -u '+%Y-%m-%d_%I:%M:%S%p')"

    # 构建二进制
    go mod tidy
    JSON_LIB=${JSON_LIB:-jsonsonic}
    GOOS=linux GOARCH=amd64 \
      go build -tags "${JSON_LIB}" -ldflags " \
        -s -w \
        -X main.version=${version} \
        -X main.buildTime=${build_time} \
        -X main.gitHash=${GIT_COMMIT_HASH} \
        -X 'main.gitTag=${GIT_TAG}' \
        -X 'main.gitCommitter=${GIT_COMMITTER_NAME}' \
        -X 'main.gitCommitTime=${GIT_COMMIT_TIME}' \
            -X 'main.buildId=${CI_BUILD_ID_VALUE}' \
        -X 'main.buildNo=${CI_BUILD_NUM_VALUE}' \
        -X 'main.release=${RELEASE_VALUE}'" \
      -o ${dist}/sidecar ./cmd/sidecar
}

function encode() {
  local base_dir="./support-files/templates/linux/x86_64/etc"

  tpls=("bk-collector-application.conf.tpl" "bk-collector-platform.conf.tpl" "bk-collector-report-v2.conf.tpl")
  # shellcheck disable=SC2068
  for tpl in ${tpls[@]}
    do echo 'File:' $tpl && cat ${base_dir}/${tpl} | base64 && echo ''
  done
}

if [ "$1" == "package" ]; then
  package $2 $3 $4 $5 $6
elif [ "$1" == "sidecar" ]; then
  sidecar $2 $3
elif [ "$1" == "test" ]; then
  unittest
elif [ "$1" == "encode" ]; then
  encode
fi
