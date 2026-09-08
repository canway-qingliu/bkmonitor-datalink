#!/usr/bin/env bash
# Tencent is pleased to support the open source community by making
# 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
# Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
# Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://opensource.org/licenses/MIT
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

set -uo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
failures=0

assert_version() {
  local name=$1
  local module=$2
  local tag=$3
  local expected=$4
  local actual

  if ! actual=$(make --no-print-directory -C "${repo_root}" \
    MODULE="${module}" \
    BUILD_GIT_TAG="${tag}" \
    GIT_REF= \
    BUILD_NO=7 \
    CI_BUILD_NUM=888 \
    version); then
    printf 'not ok - %s: make failed\n' "${name}" >&2
    failures=$((failures + 1))
    return
  fi

  if [[ "${actual}" != "${expected}" ]]; then
    printf 'not ok - %s: expected %q, got %q\n' "${name}" "${expected}" "${actual}" >&2
    failures=$((failures + 1))
    return
  fi

  printf 'ok - %s\n' "${name}"
}

assert_version "collector beta tag accepts v prefix" collector \
  beta/bk-collector/v0.105.1.200 0.105.1.200
assert_version "collector beta tag remains compatible without v prefix" collector \
  beta/bk-collector/0.105.1.200 0.105.1.200
assert_version "collector release tag accepts v prefix" collector \
  release/bk-collector/v0.105.1 0.105.1
assert_version "v-prefixed module beta tag keeps one v prefix" transfer \
  beta/transfer/v3.9.1.200 v3.9.1.200
assert_version "v-prefixed module beta tag remains compatible without input v" transfer \
  beta/transfer/3.9.1.200 v3.9.1.200
assert_version "v-prefixed module release tag keeps one v prefix" transfer \
  release/transfer/v3.9.1 v3.9.1

if ((failures > 0)); then
  printf '%d version tag test(s) failed\n' "${failures}" >&2
  exit 1
fi

printf 'all version tag tests passed\n'
