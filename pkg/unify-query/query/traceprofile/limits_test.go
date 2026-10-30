// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package traceprofile

import "testing"

func TestWindowFitsTheInclusiveMillisecondEnd(t *testing.T) {
	start := int64(1_758_585_600_000_000)
	// 调用方发 end_us=(end_ms+1)*1000：整 30 天再加 1 ms。
	if !WindowFits(start, start+30*dayUS+1000, MaxWindowUS(30)) {
		t.Fatal("the maximum product window was rejected")
	}
	if WindowFits(start, start+30*dayUS+1001, MaxWindowUS(30)) || WindowFits(start, start, MaxWindowUS(30)) {
		t.Fatal("a window past the allowance or an empty window was accepted")
	}
}
