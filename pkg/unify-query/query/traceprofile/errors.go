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
	"errors"
	"fmt"
)

// 与 KAPM 冻结的封闭错误码集合（读写契约 §5.2、T6.2）。
const (
	CodeUnauthorizedDomain          = "TRACE_UNAUTHORIZED_DOMAIN"
	CodeUnauthorizedMember          = "TRACE_UNAUTHORIZED_MEMBER"
	CodeScopePartial                = "TRACE_SCOPE_PARTIAL"
	CodePredicateConflict           = "TRACE_PREDICATE_CONFLICT"
	CodeUnboundedRange              = "TRACE_UNBOUNDED_RANGE"
	CodeLimitExceeded               = "TRACE_LIMIT_EXCEEDED"
	CodeRouteNotFound               = "TRACE_ROUTE_NOT_FOUND"
	CodeUnsafeUnion                 = "TRACE_UNSAFE_UNION"
	CodeUnsupportedSchemaCapability = "TRACE_UNSUPPORTED_SCHEMA_CAPABILITY"
	CodePartialFailure              = "TRACE_PARTIAL_FAILURE"
	CodeInvalidPageToken            = "TRACE_INVALID_PAGE_TOKEN"
)

// RejectError 是 Trace 直查的明确失败：要么拒绝请求，要么拒绝结果，
// 不静默降级、不返回伪完整数据。
type RejectError struct {
	Code    string
	Message string
}

func (e *RejectError) Error() string {
	return e.Message
}

func reject(code, format string, args ...any) error {
	return &RejectError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// ErrorCode 返回对外封闭错误码；未分类错误按服务端失败处理。
func ErrorCode(err error) string {
	var rejected *RejectError
	if errors.As(err, &rejected) && rejected.Code != "" {
		return rejected.Code
	}
	return CodePartialFailure
}
