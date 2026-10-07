// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import "fmt"

// The direct Trace reader will only accept a signed, verified Kingeye workload.
func authorizeTraceProfile(verified bool, appCode string, allowed []string) error {
	if !verified || appCode == "" {
		return fmt.Errorf("Trace profile 要求已验证的 KAPM 工作负载 JWT")
	}
	for _, candidate := range allowed {
		if candidate != "" && candidate == appCode {
			return nil
		}
	}
	return fmt.Errorf("Trace profile 工作负载身份不在专用允许列表中")
}
