// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
)

func TestRawQueryUsesESNativeHighlight(t *testing.T) {
	reference := func(storageTypes ...string) metadata.QueryReference {
		queries := make(metadata.QueryList, 0, len(storageTypes))
		for _, storageType := range storageTypes {
			queries = append(queries, &metadata.Query{StorageType: storageType})
		}
		return metadata.QueryReference{
			"a": {{
				ReferenceName: "a",
				QueryList:     queries,
			}},
		}
	}

	for _, tc := range []struct {
		name      string
		highlight *metadata.HighLight
		reference metadata.QueryReference
		expected  bool
	}{
		{
			name:      "未开启高亮",
			highlight: &metadata.HighLight{Enable: false},
			reference: reference(metadata.ElasticsearchStorageType),
			expected:  false,
		},
		{
			name:      "auto 全部 ES",
			highlight: &metadata.HighLight{Enable: true},
			reference: reference(metadata.ElasticsearchStorageType, metadata.ElasticsearchStorageType),
			expected:  true,
		},
		{
			name:      "auto 包含 Doris",
			highlight: &metadata.HighLight{Enable: true},
			reference: reference(metadata.ElasticsearchStorageType, metadata.BkSqlStorageType),
			expected:  false,
		},
		{
			name:      "auto 仅 Doris",
			highlight: &metadata.HighLight{Enable: true},
			reference: reference(metadata.BkSqlStorageType),
			expected:  false,
		},
		{
			name:      "强制 regex",
			highlight: &metadata.HighLight{Enable: true, Mode: metadata.HighLightModeRegex},
			reference: reference(metadata.ElasticsearchStorageType),
			expected:  false,
		},
		{
			name:      "强制 ES",
			highlight: &metadata.HighLight{Enable: true, Mode: metadata.HighLightModeES},
			reference: reference(metadata.ElasticsearchStorageType),
			expected:  true,
		},
		{
			name:      "强制 ES 但包含 Doris 时回退",
			highlight: &metadata.HighLight{Enable: true, Mode: metadata.HighLightModeES},
			reference: reference(metadata.ElasticsearchStorageType, metadata.BkSqlStorageType),
			expected:  false,
		},
		{
			name:      "无查询",
			highlight: &metadata.HighLight{Enable: true},
			reference: metadata.QueryReference{},
			expected:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queryTs := &structured.QueryTs{HighLight: tc.highlight}
			assert.Equal(t, tc.expected, rawQueryUsesESNativeHighlight(queryTs, tc.reference))
		})
	}
}
