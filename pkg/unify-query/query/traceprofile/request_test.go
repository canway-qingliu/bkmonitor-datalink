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
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

func validRequestJSON() string {
	return `{"trace_profile":"trace_summary","trace_contract_version":"trace_query_v2","trace_request":{` +
		`"observation_domain_id":"system_kapm",` +
		`"expected_scope_fingerprint":"` + strings.Repeat("a", 64) + `",` +
		`"scene":"app","start_time":"1721024820000000","end_time":"1721024880000000",` +
		`"filters":{"field_list":[{"field_name":"app_id","value":["7"],"op":"eq"}],"condition_list":[]},` +
		`"order_by":["-timestamp","trace_id"],"limit":25,"offset":0}}`
}

func TestParseRequestAcceptsVersionedCountAggregation(t *testing.T) {
	body := strings.Replace(validRequestJSON(), `"trace_query_v2"`, `"trace_query_v3"`, 1)
	body = strings.Replace(body, `"order_by":["-timestamp","trace_id"],"limit":25,"offset":0`,
		`"order_by":[],"limit":10000,"aggregation":{"method":"count","interval_seconds":60,`+
			`"timezone_offset_seconds":28800,"group_by":["root_service_name"]}`, 1)
	if _, err := ParseRequest([]byte(body), 1<<20, 10000); err != nil {
		t.Fatalf("bounded typed count aggregation was rejected: %v", err)
	}
	legacy := strings.Replace(body, `"trace_query_v3"`, `"trace_query_v2"`, 1)
	if _, err := ParseRequest([]byte(legacy), 1<<20, 10000); err == nil {
		t.Fatal("v2 list contract accepted aggregation extension")
	}
	for name, invalid := range map[string]string{
		"offset":     strings.Replace(body, `"limit":10000,`, `"limit":10000,"offset":0,`, 1),
		"order":      strings.Replace(body, `"order_by":[]`, `"order_by":["trace_id"]`, 1),
		"physical":   strings.Replace(body, `"aggregation":{`, `"database":"system_kapm","aggregation":{`, 1),
		"wrong mode": strings.Replace(body, `"method":"count"`, `"method":"sum"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseRequest([]byte(invalid), 1<<20, 10000); err == nil {
				t.Fatal("unsafe aggregate request was accepted")
			}
		})
	}
}

func validMemberLookupJSON() string {
	return `{"trace_profile":"trace_summary","trace_contract_version":"trace_query_v2","trace_request":{` +
		`"observation_domain_id":"t1_kapm",` +
		`"expected_scope_fingerprint":"` + strings.Repeat("a", 64) + `",` +
		`"scene":"global","start_time":"1721024820000000","end_time":"1721024880000000",` +
		`"filters":{"field_list":[],"condition_list":[]},"order_by":[],"limit":25,` +
		`"member_lookup":{"trace_id":"0123456789abcdef0123456789abcdef","expected_route_version":3,"allowed_pairs":` +
		`[{"app_id":7,"bk_biz_id":7},{"app_id":8,"bk_biz_id":7}]}}}`
}

func TestParseRequestAcceptsBoundedExactSummaryMemberLookup(t *testing.T) {
	request, err := ParseRequest([]byte(validMemberLookupJSON()), 1<<20, 100)
	if err != nil {
		t.Fatalf("bounded exact member lookup was rejected: %v", err)
	}
	if request.MemberLookup == nil || request.MemberLookup.TraceID != "0123456789abcdef0123456789abcdef" ||
		len(request.MemberLookup.AllowedPairs) != 2 || request.MemberLookup.AllowedPairs[1].AppID != 8 ||
		request.MemberLookup.Backend != MemberBackendDoris {
		t.Fatalf("member lookup lost exact identity or pairs: %+v", request.MemberLookup)
	}
}

func TestParseRequestRequiresOnePairForExplicitESMemberBackend(t *testing.T) {
	valid := strings.Replace(validMemberLookupJSON(),
		`"member_lookup":{"trace_id":`, `"member_lookup":{"backend":"elasticsearch","trace_id":`, 1)
	valid = strings.Replace(valid, `,{"app_id":8,"bk_biz_id":7}`, ``, 1)
	request, err := ParseRequest([]byte(valid), 1<<20, 100)
	if err != nil || request.MemberLookup == nil || request.MemberLookup.Backend != MemberBackendElasticsearch {
		t.Fatalf("one-pair ES member lookup was rejected: request=%+v err=%v", request, err)
	}
	for name, body := range map[string]string{
		"unknown backend":   strings.Replace(valid, `"backend":"elasticsearch"`, `"backend":"other"`, 1),
		"es multiple pairs": strings.Replace(valid, `{"app_id":7,"bk_biz_id":7}`, `{"app_id":7,"bk_biz_id":7},{"app_id":8,"bk_biz_id":7}`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseRequest([]byte(body), 1<<20, 100); err == nil {
				t.Fatal("unsupported ES member backend scope was accepted")
			}
		})
	}
}

func TestParseRequestRejectsMemberLookupScopeExpansionAndListMixing(t *testing.T) {
	valid := validMemberLookupJSON()
	cases := map[string]string{
		"null lookup":           valid[:strings.Index(valid, `"member_lookup":`)] + `"member_lookup":null}}`,
		"missing route version": strings.Replace(valid, `"expected_route_version":3,`, ``, 1),
		"zero route version":    strings.Replace(valid, `"expected_route_version":3`, `"expected_route_version":0`, 1),
		"uppercase trace":       strings.Replace(valid, "0123456789abcdef0123456789abcdef", "0123456789ABCDEF0123456789ABCDEF", 1),
		"empty pairs":           strings.Replace(valid, `[{"app_id":7,"bk_biz_id":7},{"app_id":8,"bk_biz_id":7}]`, `[]`, 1),
		"duplicate pair":        strings.Replace(valid, `{"app_id":8,"bk_biz_id":7}`, `{"app_id":7,"bk_biz_id":7}`, 1),
		"unsorted pair": strings.Replace(valid, `{"app_id":7,"bk_biz_id":7},{"app_id":8,"bk_biz_id":7}`,
			`{"app_id":8,"bk_biz_id":7},{"app_id":7,"bk_biz_id":7}`, 1),
		"list offset": strings.Replace(valid, `"limit":25,`, `"limit":25,"offset":0,`, 1),
		"list filter": strings.Replace(valid, `"field_list":[]`,
			`"field_list":[{"field_name":"app_id","value":["7"],"op":"eq"}]`, 1),
		"span profile":   strings.Replace(valid, `"trace_summary"`, `"trace_span"`, 1),
		"nested unknown": strings.Replace(valid, `"allowed_pairs":`, `"sql":"SELECT *","allowed_pairs":`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseRequest([]byte(body), 1<<20, 100); err == nil {
				t.Fatal("untrusted member lookup scope was accepted")
			}
		})
	}
}

func TestParseRequestAcceptsClosedKingeyeTraceEnvelope(t *testing.T) {
	request, err := ParseRequest([]byte(validRequestJSON()), 1<<20, 100)
	if err != nil {
		t.Fatal(err)
	}
	if request.Profile != metadata.TraceProfileSummary || request.StartUS != 1721024820000000 ||
		request.EndUS != 1721024880000000 || request.Limit != 25 || request.Offset == nil || *request.Offset != 0 {
		t.Fatalf("parsed request lost typed fields: %+v", request)
	}
	if request.ScopeFingerprint != strings.Repeat("a", 64) || request.Filters.FieldList[0].FieldName != "app_id" {
		t.Fatalf("parsed request lost scope or filters: %+v", request)
	}
}

func TestParseRequestAcceptsBoundedSpanOffset(t *testing.T) {
	body := strings.Replace(validRequestJSON(), `"trace_summary"`, `"trace_span"`, 1)
	body = strings.Replace(body, `"scene":"app",`, "", 1)
	body = strings.Replace(body, `"offset":0`, `"offset":50`, 1)
	request, err := ParseRequest([]byte(body), 1<<20, 100)
	if err != nil {
		t.Fatalf("Span page offset was rejected: %v", err)
	}
	if request.Offset == nil || *request.Offset != 50 {
		t.Fatalf("Span page offset was lost: %+v", request)
	}
}

func TestParseRequestRejectsSecondAndMillisecondEpochs(t *testing.T) {
	for _, value := range []string{"1790749515", "1790749515960"} {
		body := strings.Replace(validRequestJSON(), "1721024820000000", value, 1)
		if _, err := ParseRequest([]byte(body), 1<<20, 100); err == nil {
			t.Fatalf("accepted %s as a microsecond timestamp", value)
		}
	}
}

func TestParseRequestRejectsCoordinatesUnknownFieldsAndDuplicateKeys(t *testing.T) {
	valid := validRequestJSON()
	cases := map[string]string{
		"client table_id":   strings.Replace(valid, `"trace_profile":"trace_summary"`, `"trace_profile":"trace_summary","table_id":"42_otel_trace_summary.doris"`, 1),
		"nested SQL":        strings.Replace(valid, `"scene":"app"`, `"scene":"app","sql":"SELECT *"`, 1),
		"duplicate profile": strings.Replace(valid, `"trace_profile":"trace_summary"`, `"trace_profile":"trace_span","trace_profile":"trace_summary"`, 1),
		"invalid profile":   strings.Replace(valid, `"trace_summary"`, `"service_edge"`, 1),
		"wrong contract":    strings.Replace(valid, `"trace_query_v2"`, `"other"`, 1),
		"fractional limit":  strings.Replace(valid, `"limit":25`, `"limit":25.5`, 1),
		"quoted limit":      strings.Replace(valid, `"limit":25`, `"limit":"25"`, 1),
		"trailing document": valid + `{}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseRequest([]byte(body), 1<<20, 100); err == nil {
				t.Fatal("malformed or physical-coordinate request was accepted")
			}
		})
	}
}

func TestParseRequestRejectsInvalidTimePaginationAndFilterShape(t *testing.T) {
	valid := validRequestJSON()
	cases := map[string]string{
		"reverse range":    strings.Replace(valid, `"end_time":"1721024880000000"`, `"end_time":"1721024820000000"`, 1),
		"non microseconds": strings.Replace(valid, `"start_time":"1721024820000000"`, `"start_time":"not-time"`, 1),
		"over limit":       strings.Replace(valid, `"limit":25`, `"limit":101`, 1),
		"bad connector":    strings.Replace(valid, `"condition_list":[]`, `"condition_list":["or"]`, 1),
		"bad operator":     strings.Replace(valid, `"op":"eq"`, `"op":"OR 1=1"`, 1),
		"offset and token": strings.Replace(valid, `"offset":0`, `"offset":0,"page_token":"opaque"`, 1),
		"null page token":  strings.Replace(valid, `"offset":0`, `"offset":0,"page_token":null`, 1),
		"null offset":      strings.Replace(valid, `"offset":0`, `"offset":null`, 1),
		"missing exists value": strings.Replace(valid,
			`{"field_name":"app_id","value":["7"],"op":"eq"}`,
			`{"field_name":"app_id","op":"existed"}`, 1),
		"null exists value": strings.Replace(valid,
			`{"field_name":"app_id","value":["7"],"op":"eq"}`,
			`{"field_name":"app_id","value":null,"op":"existed"}`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseRequest([]byte(body), 1<<20, 100); err == nil {
				t.Fatal("invalid Trace range, pagination or filter was accepted")
			}
		})
	}
	spanWithNullScene := strings.Replace(valid, `"trace_summary"`, `"trace_span"`, 1)
	spanWithNullScene = strings.Replace(spanWithNullScene, `"scene":"app"`, `"scene":null`, 1)
	spanWithNullScene = strings.Replace(spanWithNullScene, `,"offset":0`, ``, 1)
	if _, err := ParseRequest([]byte(spanWithNullScene), 1<<20, 100); err == nil {
		t.Fatal("Span request with explicit null scene was accepted")
	}
	if _, err := ParseRequest([]byte(valid), len(valid)-1, 100); err == nil {
		t.Fatal("oversized request was accepted")
	}
}

func TestRequestDuplicateScannerRejectsExcessiveNesting(t *testing.T) {
	deep := []byte(strings.Repeat("[", 80) + "0" + strings.Repeat("]", 80))
	if err := rejectDuplicateKeys(deep); err == nil {
		t.Fatal("deeply nested JSON was accepted")
	}
}
