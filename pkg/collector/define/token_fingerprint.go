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
	"crypto/sha256"
	"encoding/hex"
)

// TokenFingerprintLabel is the label a self-monitoring metric names an
// application's token under.
const TokenFingerprintLabel = "token_fingerprint"

// TokenFingerprint is how a self-monitoring metric names a token: the hex of
// the first eight bytes of its SHA-256. A token is a credential, and a label
// is scraped, stored and shown on dashboards; the fingerprint tells tokens
// apart and cannot be turned back into one. receiver_token_info maps a
// fingerprint to its application and data ids. A request that carried no
// token stays empty rather than taking the fingerprint of the empty string.
func TokenFingerprint(original string) string {
	if original == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(original))
	return hex.EncodeToString(digest[:8])
}
