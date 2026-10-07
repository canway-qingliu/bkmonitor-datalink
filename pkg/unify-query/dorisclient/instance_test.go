// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package dorisclient

import (
	"strings"
	"testing"
)

func TestValidateStatementOnlyAllowsSingleReadOnlySelect(t *testing.T) {
	cases := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{name: "select", statement: "SELECT trace_id FROM otel_trace_span WHERE start_time_us >= ?"},
		{name: "with", statement: "WITH scope AS (SELECT trace_id FROM otel_trace_span) SELECT trace_id FROM scope"},
		{name: "empty", statement: "   ", wantErr: "语句为空"},
		{name: "insert", statement: "INSERT INTO otel_trace_span VALUES (1)", wantErr: "只允许 SELECT/WITH"},
		{name: "update", statement: "UPDATE otel_trace_span SET app_name = 'x'", wantErr: "只允许 SELECT/WITH"},
		{name: "drop", statement: "DROP TABLE otel_trace_span", wantErr: "只允许 SELECT/WITH"},
		{name: "multi statement", statement: "SELECT 1; DROP TABLE otel_trace_span", wantErr: "禁用片段"},
		{name: "trailing separator", statement: "SELECT 1;", wantErr: "禁用片段"},
		{name: "line comment", statement: "SELECT 1 -- comment", wantErr: "禁用片段"},
		{name: "block comment", statement: "SELECT /* probe */ 1", wantErr: "禁用片段"},
		{name: "hash comment", statement: "SELECT 1 # probe", wantErr: "禁用片段"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateStatement(tc.statement)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected statement accepted, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestNormalizeValueKeepsOnlyScalars(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  any
	}{
		{name: "null", value: nil, want: nil},
		{name: "bytes to string", value: []byte("bkcc__42"), want: "bkcc__42"},
		{name: "string", value: "trace", want: "trace"},
		{name: "int64", value: int64(7), want: int64(7)},
		{name: "float64", value: float64(1.5), want: float64(1.5)},
		{name: "bool", value: true, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := normalizeValue(tc.value)
			if err != nil {
				t.Fatalf("normalizeValue(%v) returned error: %v", tc.value, err)
			}
			if value != tc.want {
				t.Fatalf("normalizeValue(%v) = %v, want %v", tc.value, value, tc.want)
			}
		})
	}

	t.Run("unsupported type fails closed", func(t *testing.T) {
		if _, err := normalizeValue([]int{1}); err == nil {
			t.Fatal("expected unsupported value rejected")
		}
	})
}

func TestNewRejectsIncompleteConnectionOptions(t *testing.T) {
	cases := []struct {
		name    string
		options Options
		wantErr string
	}{
		{name: "missing cluster", options: Options{Host: "127.0.0.1", Port: 9030, Username: "readonly"}, wantErr: "集群"},
		{name: "missing host", options: Options{ClusterID: "18", Port: 9030, Username: "readonly"}, wantErr: "集群"},
		{name: "missing port", options: Options{ClusterID: "18", Host: "127.0.0.1", Username: "readonly"}, wantErr: "集群"},
		{name: "missing username", options: Options{ClusterID: "18", Host: "127.0.0.1", Port: 9030}, wantErr: "只读用户名"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instance, err := New(tc.options)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
			if instance != nil {
				t.Fatal("expected no instance on rejected options")
			}
		})
	}

	t.Run("valid options without server", func(t *testing.T) {
		instance, err := New(Options{ClusterID: "18", Host: "127.0.0.1", Port: 9030, Username: "readonly"})
		if err != nil {
			t.Fatalf("New returned error: %v", err)
		}
		if instance.ClusterID() != "18" {
			t.Fatalf("unexpected cluster id %q", instance.ClusterID())
		}
		if instance.queryTimeout != defaultQueryTimeout || instance.maxRows != defaultMaxRows {
			t.Fatalf("defaults not applied: timeout=%s maxRows=%d", instance.queryTimeout, instance.maxRows)
		}
		if err := instance.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	})
}

func TestResultTypeOfReportsColumnTypes(t *testing.T) {
	result := Result{Columns: []Column{{Name: "app_id", Type: "VARCHAR"}, {Name: "start_time_us", Type: "BIGINT"}}}
	if result.TypeOf("app_id") != "VARCHAR" || result.TypeOf("start_time_us") != "BIGINT" ||
		result.TypeOf("missing") != "" {
		t.Fatalf("unexpected column types: %+v", result.Columns)
	}
}
