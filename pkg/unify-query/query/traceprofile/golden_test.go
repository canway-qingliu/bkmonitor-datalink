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
	"encoding/json"
	"os"
	"reflect"
	"testing"

	uqjson "github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/internal/json"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

type traceRowGoldenCase struct {
	Profile      metadata.TraceProfile `json:"profile"`
	PhysicalRow  map[string]any        `json:"physical_row"`
	ExpectedItem json.RawMessage       `json:"expected_item"`
}

type traceRowGoldenFixture struct {
	Schema               string               `json:"schema"`
	TraceContractVersion string               `json:"trace_contract_version"`
	Cases                []traceRowGoldenCase `json:"cases"`
	EmptyPages           []struct {
		Profile metadata.TraceProfile `json:"profile"`
		Total   int64                 `json:"total"`
		Offset  int64                 `json:"offset"`
		Items   []any                 `json:"items"`
	} `json:"empty_pages"`
}

func decodeGoldenJSON(t *testing.T, input []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestTraceProfileRowGoldenV2MatchesUQPhysicalRowShaping(t *testing.T) {
	input, err := os.ReadFile("testdata/trace-profile-row-golden-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	var fixture traceRowGoldenFixture
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Schema != "trace-profile-row-golden-v2" || fixture.TraceContractVersion != "trace_query_v2" ||
		len(fixture.Cases) != 2 || len(fixture.EmptyPages) != 2 {
		t.Fatalf("row golden shape drifted: %+v", fixture)
	}
	for _, testCase := range fixture.Cases {
		t.Run(string(testCase.Profile), func(t *testing.T) {
			items, err := ShapeRows(testCase.Profile, []map[string]any{testCase.PhysicalRow})
			if err != nil || len(items) != 1 {
				t.Fatalf("golden physical row was rejected: items=%+v err=%v", items, err)
			}
			wire, err := uqjson.Marshal(items[0])
			if err != nil {
				t.Fatal(err)
			}
			if got, want := decodeGoldenJSON(t, wire), decodeGoldenJSON(t, testCase.ExpectedItem); !reflect.DeepEqual(got, want) {
				t.Fatalf("UQ typed item differs from shared golden:\ngot=%s\nwant=%s", wire, testCase.ExpectedItem)
			}
		})
	}
	for _, page := range fixture.EmptyPages {
		items, err := ShapeRows(page.Profile, []map[string]any{})
		if err != nil || len(items) != 0 || len(page.Items) != 0 || page.Total < 0 ||
			page.Profile == metadata.TraceProfileSpan && (page.Total != 0 || page.Offset != 0) ||
			page.Profile == metadata.TraceProfileSummary && page.Offset == 0 && page.Total > 0 {
			t.Fatalf("empty page golden is not admissible: %+v err=%v", page, err)
		}
	}
}
