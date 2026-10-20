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
//
// Message 是返回给调用方的封闭文案；Cause 是服务端原因（驱动或后端错误原文，
// 可能含后端地址），只写服务端日志，不进入响应体或 span。
type RejectError struct {
	Code    string
	Message string
	Cause   error
}

func (e *RejectError) Error() string {
	return e.Message
}

func (e *RejectError) Unwrap() error {
	return e.Cause
}

// RejectCause 返回 err 携带的服务端原因，没有时为 nil。
func RejectCause(err error) error {
	var rejected *RejectError
	if errors.As(err, &rejected) {
		return rejected.Cause
	}
	return err
}

// knownCodes 是封闭错误码集合，指标标签只取其中的值。
var knownCodes = map[string]struct{}{
	CodeUnauthorizedDomain: {}, CodeUnauthorizedMember: {}, CodeScopePartial: {}, CodePredicateConflict: {},
	CodeUnboundedRange: {}, CodeLimitExceeded: {}, CodeRouteNotFound: {}, CodeUnsafeUnion: {},
	CodeUnsupportedSchemaCapability: {}, CodePartialFailure: {}, CodeInvalidPageToken: {},
}

// IsKnownCode 报告 code 是否属于封闭错误码集合。
func IsKnownCode(code string) bool {
	_, known := knownCodes[code]
	return known
}

// IsBackendFailure 报告该错误码是否表示后端（存储读取或结果复核）失败，而不是请求被拒。
func IsBackendFailure(code string) bool {
	return code == CodePartialFailure
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
