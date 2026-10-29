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

func observedApplicationTopologyRequestJSON(fingerprint string) []byte {
	return []byte(fmt.Sprintf(`{"application_topology_contract_version":"application_topology_range_v2","application_topology_request":{
"expected_scope_fingerprint":"%s",
"expected_route_version":4,"start_time_ms":1760572800000,"end_time_ms":1760573700000,
"applications":[{"namespace_id":11,"bk_biz_id":2},{"namespace_id":12,"bk_biz_id":3}],"max_pairs":10000}}`, fingerprint))
}

func TestApplicationTopologyObservedEdgesFingerprintMatchesPythonContract(t *testing.T) {
	const expected = "19770c654691835e30327afc88978d52730169a20f4c91fb20b5782a467e303b"
	query, err := ParseApplicationTopologyRangeRequest(observedApplicationTopologyRequestJSON(expected), 65536)
	if err != nil || query.Request.ContractVersion != observedApplicationTopologyRangeVersion {
		t.Fatalf("observed edge request rejected: %+v, %v", query, err)
	}
	if got := ApplicationTopologyRangeFingerprint("tenant_a", query); got != expected {
		t.Fatalf("application topology scope fingerprint mismatch: %s", got)
	}
}

func TestApplicationTopologyRangeRequestRejectsUnknownAndDuplicateFields(t *testing.T) {
	const expected = "19770c654691835e30327afc88978d52730169a20f4c91fb20b5782a467e303b"
	for _, body := range [][]byte{
		[]byte(strings.Replace(string(observedApplicationTopologyRequestJSON(expected)),
			`"max_pairs":10000`, `"max_pairs":10000,"database":"private"`, 1)),
		[]byte(strings.Replace(string(observedApplicationTopologyRequestJSON(expected)),
			`"max_pairs":10000`, `"max_pairs":10000,"max_pairs":10000`, 1)),
	} {
		if _, err := ParseApplicationTopologyRangeRequest(body, 65536); err == nil {
			t.Fatal("unsafe application topology request accepted")
		}
	}
}
