// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package skywalkingprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMeterDataActionMatch(t *testing.T) {
	action := MeterDataAction{
		Enabled: true,
		Rules: []MetricRule{
			{
				Types: []string{"*"},
				Metrics: map[string]string{
					"activeConnections": "commonActiveConnections",
				},
			},
			{
				Types: []string{"dbcp-2.x"},
				Metrics: map[string]string{
					"activeConnections": "dbcpActiveConnections",
					"numActive":         "activeConnections",
				},
			},
		},
	}
	require.NoError(t, action.Validate())

	tests := []struct {
		name        string
		framework   string
		metric      string
		destination string
		matched     bool
	}{
		{
			name:        "exact mapping has priority",
			framework:   "dbcp-2.x",
			metric:      "activeConnections",
			destination: "dbcpActiveConnections",
			matched:     true,
		},
		{
			name:        "exact alias mapping",
			framework:   "dbcp-2.x",
			metric:      "numActive",
			destination: "activeConnections",
			matched:     true,
		},
		{
			name:        "fallback to wildcard mapping",
			framework:   "hikaricp-3.x-4.x",
			metric:      "activeConnections",
			destination: "commonActiveConnections",
			matched:     true,
		},
		{name: "missing framework does not match wildcard", metric: "activeConnections"},
		{name: "unknown metric", framework: "dbcp-2.x", metric: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			destination, matched := action.Match(tt.framework, tt.metric)
			assert.Equal(t, tt.matched, matched)
			assert.Equal(t, tt.destination, destination)
		})
	}
}

func TestMeterDataActionValidate(t *testing.T) {
	tests := []struct {
		name        string
		action      MeterDataAction
		errContains string
	}{
		{
			name:        "enabled action requires rules",
			action:      MeterDataAction{Enabled: true},
			errContains: "no rules specified",
		},
		{
			name: "types is required",
			action: MeterDataAction{Rules: []MetricRule{
				{Metrics: map[string]string{"source": "destination"}},
			}},
			errContains: "types is required",
		},
		{
			name: "metrics is required",
			action: MeterDataAction{Rules: []MetricRule{
				{Types: []string{"dbcp-2.x"}},
			}},
			errContains: "metrics is required",
		},
		{
			name: "wildcard cannot be mixed with exact type",
			action: MeterDataAction{Rules: []MetricRule{
				{
					Types:   []string{"*", "dbcp-2.x"},
					Metrics: map[string]string{"source": "destination"},
				},
			}},
			errContains: "cannot mix wildcard",
		},
		{
			name: "duplicated exact mapping",
			action: MeterDataAction{Rules: []MetricRule{
				{
					Types:   []string{"dbcp-2.x"},
					Metrics: map[string]string{"source": "destination"},
				},
				{
					Types:   []string{"dbcp-2.x"},
					Metrics: map[string]string{"source": "another"},
				},
			}},
			errContains: "duplicated mapping",
		},
		{
			name: "duplicated wildcard mapping",
			action: MeterDataAction{Rules: []MetricRule{
				{
					Types:   []string{"*"},
					Metrics: map[string]string{"source": "destination"},
				},
				{
					Types:   []string{"*"},
					Metrics: map[string]string{"source": "another"},
				},
			}},
			errContains: "duplicated mapping",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.action.Validate()
			assert.ErrorContains(t, err, tt.errContains)
		})
	}
}
