// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package traceprofile

const (
	dayUS int64 = 86_400_000_000

	// WindowEndAllowanceUS 是窗口跨度的结束点余量：调用方产品窗口的毫秒结束点含端点，
	// 换成排他的微秒结束点后比整天数多出 1 ms。
	WindowEndAllowanceUS int64 = 1000

	// MaxRangeDays 是 typed Trace 合同允许的最大窗口天数。
	MaxRangeDays int64 = 30
)

// MaxWindowUS 是 days 天窗口允许的最大跨度（微秒，含结束点余量）。
func MaxWindowUS(days int64) int64 {
	return days*dayUS + WindowEndAllowanceUS
}

// WindowFits 报告 [startUS, endUS) 非空且跨度不超过 maxUS。所有时间窗检查都经过这里。
func WindowFits(startUS, endUS, maxUS int64) bool {
	return endUS > startUS && endUS-startUS <= maxUS
}
