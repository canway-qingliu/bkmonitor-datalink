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
	"fmt"
	"strings"
	"testing"
)

func legacyPanoramaRequestJSON(fingerprint string) []byte {
	return []byte(fmt.Sprintf(`{"a3_contract_version":"a3_panorama_range_v1","a3_request":{
"expected_scope_fingerprint":"%s","observation_domain_id":"tenant_a_kapm",
"expected_route_version":4,"start_time_ms":1760572800000,"end_time_ms":1760573700000,
"applications":[{"namespace_id":11,"bk_biz_id":2},{"namespace_id":12,"bk_biz_id":3}],"max_pairs":10000}}`, fingerprint))
}

func TestApplicationTopologyRangeRequestFingerprintMatchesPythonContract(t *testing.T) {
	const expected = "90215fae3664992ddaa312247d35b6c183020a30a72e6ac01005b08b59c192e3"
	query, err := ParseApplicationTopologyRangeRequest(legacyPanoramaRequestJSON(expected), 65536)
	if err != nil {
		t.Fatal(err)
	}
	if got := ApplicationTopologyRangeFingerprint("tenant_a", query); got != expected {
		t.Fatalf("scope drifted: %s", got)
	}
}

func TestApplicationTopologyEnvelopeUsesDomainContractAndFingerprint(t *testing.T) {
	const expected = "87bb338c01c6a7c6b6248a58f71af244fa4e4da815d98d5badb181c8ff4a8b82"
	body := strings.ReplaceAll(string(legacyPanoramaRequestJSON(expected)),
		"a3_contract_version", "application_topology_contract_version")
	body = strings.ReplaceAll(body, "a3_request", "application_topology_request")
	body = strings.ReplaceAll(body, "a3_panorama_range_v1", "application_topology_range_v1")
	query, err := ParseApplicationTopologyRangeRequest([]byte(body), 65536)
	if err != nil {
		t.Fatalf("application topology request rejected: %v", err)
	}
	if got := ApplicationTopologyRangeFingerprint("tenant_a", query); got != expected {
		t.Fatalf("application topology scope fingerprint mismatch: %s", got)
	}
}

func TestApplicationTopologyRangeRequestRejectsUnknownAndDuplicateFields(t *testing.T) {
	const expected = "90215fae3664992ddaa312247d35b6c183020a30a72e6ac01005b08b59c192e3"
	for _, body := range [][]byte{
		[]byte(strings.Replace(string(legacyPanoramaRequestJSON(expected)), `"max_pairs":10000`, `"max_pairs":10000,"database":"private"`, 1)),
		[]byte(strings.Replace(string(legacyPanoramaRequestJSON(expected)), `"max_pairs":10000`, `"max_pairs":10000,"max_pairs":10000`, 1)),
	} {
		if _, err := ParseApplicationTopologyRangeRequest(body, 65536); err == nil {
			t.Fatal("unsafe request accepted")
		}
	}
}
