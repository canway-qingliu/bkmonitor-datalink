// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import "testing"

func TestTraceProfileRequiresVerifiedAllowlistedWorkload(t *testing.T) {
	cases := []struct {
		name     string
		verified bool
		appCode  string
		allowed  []string
		wantOK   bool
	}{
		{name: "legacy JWT optional but signed workload verified", verified: true, appCode: "kingeye-apm", allowed: []string{"kingeye-apm"}, wantOK: true},
		{name: "unverified header", appCode: "kingeye-apm", allowed: []string{"kingeye-apm"}},
		{name: "empty allowlist", verified: true, appCode: "kingeye-apm"},
		{name: "foreign workload", verified: true, appCode: "other", allowed: []string{"kingeye-apm"}},
		{name: "trusted workload", verified: true, appCode: "kingeye-apm", allowed: []string{"kingeye-apm"}, wantOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := authorizeTraceProfile(tc.verified, tc.appCode, tc.allowed)
			if (err == nil) != tc.wantOK {
				t.Fatalf("typed Trace workload gate = %v, want authorized=%v", err, tc.wantOK)
			}
		})
	}
}
