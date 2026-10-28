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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type ApplicationTopologyRangeQuery struct {
	Request              ApplicationTopologyRangeRequest
	ObservationDomainID  string
	ExpectedRouteVersion int64
	ScopeFingerprint     string
	ProtocolVersion      string
}

type applicationTopologyEnvelope struct {
	Version string                        `json:"application_topology_contract_version"`
	Request *applicationTopologyInputWire `json:"application_topology_request"`
}

const observedApplicationTopologyRangeVersion = "application_topology_range_v2"

type applicationTopologyInputWire struct {
	ScopeFingerprint     string                           `json:"expected_scope_fingerprint"`
	ObservationDomainID  string                           `json:"observation_domain_id"`
	ExpectedRouteVersion int64                            `json:"expected_route_version"`
	StartMS              int64                            `json:"start_time_ms"`
	EndMS                int64                            `json:"end_time_ms"`
	Applications         []ApplicationTopologyApplication `json:"applications"`
	MaxPairs             int64                            `json:"max_pairs"`
}

// ParseApplicationTopologyRangeRequest 只解析冻结的 typed 合同，不允许请求携带租户或物理表。
func ParseApplicationTopologyRangeRequest(body []byte, maxBodyBytes int) (ApplicationTopologyRangeQuery, error) {
	if maxBodyBytes <= 0 || len(body) == 0 || len(body) > maxBodyBytes {
		return ApplicationTopologyRangeQuery{}, fmt.Errorf("应用拓扑 全景请求体超出预算")
	}
	if err := rejectDuplicateKeys(body); err != nil {
		return ApplicationTopologyRangeQuery{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var envelope applicationTopologyEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return ApplicationTopologyRangeQuery{}, fmt.Errorf("应用拓扑 全景请求不是闭合 JSON")
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return ApplicationTopologyRangeQuery{}, fmt.Errorf("应用拓扑 全景请求包含多余内容")
	}
	version, item := envelope.Version, envelope.Request
	if version != observedApplicationTopologyRangeVersion {
		return ApplicationTopologyRangeQuery{}, fmt.Errorf("应用拓扑合同版本不受支持")
	}
	if item == nil {
		return ApplicationTopologyRangeQuery{}, fmt.Errorf("应用拓扑 全景合同版本不受支持")
	}
	if item.ObservationDomainID == "" || !physicalIdentifierPattern.MatchString(item.ObservationDomainID) ||
		item.ExpectedRouteVersion <= 0 || len(item.ScopeFingerprint) != 64 ||
		len(item.Applications) == 0 || len(item.Applications) > 129 ||
		item.MaxPairs < 1 || item.MaxPairs > 10000 ||
		item.StartMS < 946684800000 || item.StartMS%60000 != 0 || item.EndMS%60000 != 0 ||
		item.EndMS <= item.StartMS || item.EndMS-item.StartMS > 30*24*60*60000 {
		return ApplicationTopologyRangeQuery{}, fmt.Errorf("应用拓扑 全景范围、应用或预算非法")
	}
	previous := int64(0)
	for _, app := range item.Applications {
		if app.NamespaceID <= previous || app.BkBizID <= 0 {
			return ApplicationTopologyRangeQuery{}, fmt.Errorf("应用拓扑 全景应用目录不是严格升序")
		}
		previous = app.NamespaceID
	}
	return ApplicationTopologyRangeQuery{
		Request:              ApplicationTopologyRangeRequest{ContractVersion: version, ExpectedVersion: item.ExpectedRouteVersion, StartMS: item.StartMS, EndMS: item.EndMS, Applications: item.Applications, MaxPairs: item.MaxPairs},
		ObservationDomainID:  item.ObservationDomainID,
		ExpectedRouteVersion: item.ExpectedRouteVersion,
		ScopeFingerprint:     item.ScopeFingerprint,
		ProtocolVersion:      version,
	}, nil
}

// ApplicationTopologyRangeFingerprint 是 Python/Go 共用的固定 ASCII 摘要输入，不依赖 JSON 键顺序。
func ApplicationTopologyRangeFingerprint(tenantID string, query ApplicationTopologyRangeQuery) string {
	parts := []string{"application-topology-range-v2", tenantID, query.ObservationDomainID,
		strconv.FormatInt(query.ExpectedRouteVersion, 10),
		strconv.FormatInt(query.Request.StartMS, 10), strconv.FormatInt(query.Request.EndMS, 10),
		strconv.FormatInt(query.Request.MaxPairs, 10)}
	for _, app := range query.Request.Applications {
		parts = append(parts, strconv.FormatInt(app.NamespaceID, 10)+":"+strconv.FormatInt(app.BkBizID, 10))
	}
	hash := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(hash[:])
}
