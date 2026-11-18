// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package define

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/tokentest"
)

// A token is named by its fingerprint, and a request that carried none by
// nothing: the empty string's own fingerprint would read as a token.
func TestATokenIsNamedByItsFingerprint(t *testing.T) {
	if got := TokenFingerprint(""); got != "" {
		t.Fatalf("TokenFingerprint(\"\") = %q, want empty", got)
	}
	const original = "fingerprint-of-a-token"
	if got, want := TokenFingerprint(original), tokentest.Fingerprint(original); got != want {
		t.Fatalf("TokenFingerprint() = %q, want %q", got, want)
	}
}

// receiver_token_info names a token by its fingerprint beside the
// application and data ids it maps to, never by the token.
func TestTheTokenInfoNamesATokenByItsFingerprint(t *testing.T) {
	token := Token{Original: "define-token-info-test", MetricsDataId: 1001, TracesDataId: 1002, AppName: "orders", BizId: 2}
	SetTokenInfo(token)
	tokentest.CheckFamilies(t, token.Original, "bk_collector_receiver_token_info")
}
