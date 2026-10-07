// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package traceprofile

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

// ContractVersion 是 KAPM 冻结的 typed Trace 读合同版本，响应必须原样回执。
func ContractVersion() string {
	return contractVersion
}

// Response 是封闭的 7 字段响应信封；多一个字段、少一个字段都是合同违规。
type Response struct {
	TraceContractVersion string                `json:"trace_contract_version"`
	TraceProfile         metadata.TraceProfile `json:"trace_profile"`
	ScopeFingerprint     string                `json:"scope_fingerprint"`
	ProductState         any                   `json:"product_state"`
	Items                []map[string]any      `json:"items"`
	Total                int64                 `json:"total"`
	NextPageToken        any                   `json:"next_page_token"`
}

// NewResponse 用请求回执与已整形的分页构造响应。
// product_state 由 KAPM 的成员向量推导，UQ 既不持有成员名单也不臆造存储状态，
// 因此恒为 null：这是 T6.2/T5 的待决接缝，宁可在 KAPM 侧显式失败也不伪造状态。
func NewResponse(request Request, page Page) Response {
	items := page.Items
	if items == nil {
		items = make([]map[string]any, 0)
	}
	return Response{
		TraceContractVersion: contractVersion,
		TraceProfile:         request.Profile,
		ScopeFingerprint:     request.ScopeFingerprint,
		ProductState:         nil,
		Items:                items,
		Total:                page.Total,
		NextPageToken:        nil,
	}
}
