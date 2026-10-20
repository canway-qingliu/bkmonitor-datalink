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

	// ESSummaryMaxRows 与 ESSummaryMaxOffset 是 ES Summary 读取的行数与 offset 上限；
	// 运行时取它们与本地配置预算中较小的一个（NewESSummaryLimits）。
	ESSummaryMaxRows   int64 = 1000
	ESSummaryMaxOffset int64 = 100000
)

// MaxWindowUS 是 days 天窗口允许的最大跨度（微秒，含结束点余量）。
func MaxWindowUS(days int64) int64 {
	return days*dayUS + WindowEndAllowanceUS
}

// WindowFits 报告 [startUS, endUS) 非空且跨度不超过 maxUS。所有时间窗检查都经过这里。
func WindowFits(startUS, endUS, maxUS int64) bool {
	return endUS > startUS && endUS-startUS <= maxUS
}

// ESSummaryLimits 是 ES Summary 成员与列表读取的有效预算，运行时构建时确定一次。
type ESSummaryLimits struct {
	MaxRows   int64
	MaxOffset int64
}

// NewESSummaryLimits 把本地配置的通用预算收敛到 ES Summary 自身的上限。
func NewESSummaryLimits(maxLimit, maxOffset int64) ESSummaryLimits {
	return ESSummaryLimits{MaxRows: min(maxLimit, ESSummaryMaxRows), MaxOffset: min(maxOffset, ESSummaryMaxOffset)}
}

// ValidateESSummaryListRequest 是 ES Summary 列表的全部预算、窗口、排序与父关系能力检查，
// 查询前与成页时共用，按原因给出封闭错误码。身份与发布版本由调用方另行校验。
func ValidateESSummaryListRequest(request Request, predicate ESSummaryListPredicate, target ESSummaryTarget, limits ESSummaryLimits) error {
	if target.RetentionDays < 1 || target.RetentionDays > MaxRangeDays {
		return reject(CodeScopePartial, "ES Summary 已发布保留期非法")
	}
	if !WindowFits(request.StartUS, request.EndUS, MaxWindowUS(target.RetentionDays)) {
		return reject(CodeScopePartial, "ES Summary 已发布保留期不足以覆盖请求")
	}
	if request.Limit < 1 || request.Limit > limits.MaxRows {
		return reject(CodeLimitExceeded, "ES Summary 列表行数超出预算")
	}
	if request.Offset != nil && (*request.Offset < 0 || *request.Offset > limits.MaxOffset) {
		return reject(CodeLimitExceeded, "ES Summary 列表 offset 超出预算")
	}
	if !validESSummaryListOrder(request.OrderBy) {
		return reject(CodeUnsupportedSchemaCapability, "ES Summary 列表必须按开始时间或耗时与 Trace ID 稳定排序")
	}
	if (predicate.ParentState == "observed" || predicate.ParentState == "missing") &&
		(target.EntryIndexVersion != 2 || target.RetentionDays != MaxRangeDays) {
		return reject(CodeUnsupportedSchemaCapability, "ES Summary 跨应用父关系索引尚未发布")
	}
	return nil
}

// ESSummaryListPredicate 返回请求的入口筛选：ParseRequest 已解析过时直接复用。
func (r Request) ListPredicate() (ESSummaryListPredicate, error) {
	if r.esSummaryPredicate != nil {
		return *r.esSummaryPredicate, nil
	}
	return ParseESSummaryListPredicate(r.Filters)
}
