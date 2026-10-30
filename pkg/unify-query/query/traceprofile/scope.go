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
	"regexp"
	"strconv"
)

// Scope 只由受信 transport 载体解析：租户与空间来自已验证请求头，
// UQ 不持有成员名单，app_id 是否属于调用方由 KAPM 承担；
// 这里只强制可信 tenant / space 边界。
type Scope struct {
	BkTenantID string
	SpaceUID   string
	BkBizID    int64
}

var spaceUIDPattern = regexp.MustCompile(`^bkcc__([0-9]{1,18})$`)
var tenantIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidTenantID 报告 id 是否是 ResolveScope 接受的租户标识。
func ValidTenantID(id string) bool {
	return tenantIDPattern.MatchString(id)
}

// ResolveScope 是 fail closed 的唯一入口：作用域缺失或与冻结命名漂移即拒绝。
func ResolveScope(tenantID, spaceUID string) (Scope, error) {
	if !ValidTenantID(tenantID) {
		return Scope{}, reject(CodeUnauthorizedMember, "Trace 请求缺少可信租户")
	}
	if !physicalIdentifierPattern.MatchString(spaceUID) {
		return Scope{}, reject(CodeUnauthorizedMember, "Trace 请求缺少可信空间")
	}
	matched := spaceUIDPattern.FindStringSubmatch(spaceUID)
	if matched == nil {
		return Scope{}, reject(CodeUnauthorizedMember, "Trace 请求空间标识非法: %s", spaceUID)
	}
	bizID, err := strconv.ParseInt(matched[1], 10, 64)
	if err != nil || bizID < 0 {
		return Scope{}, reject(CodeUnauthorizedMember, "Trace 请求空间标识非法: %s", spaceUID)
	}
	return Scope{BkTenantID: tenantID, SpaceUID: spaceUID, BkBizID: bizID}, nil
}

type identityPredicate struct {
	column string
	value  any
}

// isolationPredicates 返回 Span 物理表必须精确匹配的隔离列。
func (s Scope) isolationPredicates() []identityPredicate {
	return []identityPredicate{
		{column: "space_uid", value: s.SpaceUID},
		{column: "bk_biz_id", value: s.BkBizID},
	}
}

// trustedIdentityValue 返回可被信任的作用域列取值；app_id / data_id 没有可信取值，
// 只允许作为收窄条件出现。
func (s Scope) trustedIdentityValue(column string) (any, bool) {
	switch column {
	case "space_uid":
		return s.SpaceUID, true
	case "bk_biz_id":
		return s.BkBizID, true
	default:
		return nil, false
	}
}
