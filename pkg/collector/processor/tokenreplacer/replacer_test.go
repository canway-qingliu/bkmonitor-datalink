// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package tokenreplacer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
)

func TestReplacer_ReplaceToken(t *testing.T) {
	const (
		token1 = "Ymtia2JrYmtia2JrYmtiaxUtdLzrldhHtlcjc1Cwfo1u99rVk5HGe8EjT761brGtKm3H4Ran78rWl85HwzfRgw=="
		token2 = "Ymtia2JrYmtia2JrYmtia/0ZJ3tXGU6OT2oEqyruVbvWr0kNl7AzgSWPsnVzNBYWRULf8XE/mtQBHLas+jYCrw=="
	)

	t.Run("app_name: replace from resource attribute", func(t *testing.T) {
		config := Config{
			Type:         typeAppName,
			resourceKeys: []string{"bk.data.token"},
			appTokenMapping: map[string]define.Token{
				"app1": {AppName: "app1", Original: token1},
			},
		}

		record := &define.Record{
			Token: define.Token{Original: "original_token"},
		}

		attrs := pcommon.NewMap()
		attrs.UpsertString("bk.data.token", "app1")

		replacer := NewReplacer()
		replacer.replaceToken(config, record, attrs)

		assert.Equal(t, token1, record.Token.Original)
		value, ok := attrs.Get("bk.data.token")
		assert.True(t, ok)
		assert.Equal(t, token1, value.AsString())
	})

	t.Run("app_name: replace from original token when attribute empty", func(t *testing.T) {
		config := Config{
			Type:         typeAppName,
			resourceKeys: []string{"bk.data.token"},
			appTokenMapping: map[string]define.Token{
				token2: {AppName: "app2", Original: token1},
			},
		}

		record := &define.Record{
			Token: define.Token{Original: token2},
		}

		attrs := pcommon.NewMap()

		replacer := NewReplacer()
		replacer.replaceToken(config, record, attrs)

		assert.Equal(t, token1, record.Token.Original)
	})

	t.Run("app_name: no replacement when not found", func(t *testing.T) {
		config := Config{
			Type:            typeAppName,
			resourceKeys:    []string{"bk.data.token"},
			appTokenMapping: map[string]define.Token{},
		}

		record := &define.Record{
			Token: define.Token{Original: "unknown_token"},
		}

		attrs := pcommon.NewMap()
		attrs.UpsertString("bk.data.token", "unknown_app")

		replacer := NewReplacer()
		replacer.replaceToken(config, record, attrs)

		assert.Equal(t, "unknown_token", record.Token.Original)
		value, ok := attrs.Get("bk.data.token")
		assert.True(t, ok)
		assert.Equal(t, "unknown_app", value.AsString())
	})

	t.Run("token_mapping: replace from resource attribute", func(t *testing.T) {
		config := Config{
			Type:         typeTokenMapping,
			resourceKeys: []string{"bk.data.token"},
			replaceMapping: map[string]string{
				"token1": "tokena",
			},
		}

		record := &define.Record{
			Token: define.Token{Original: "original_token"},
		}

		attrs := pcommon.NewMap()
		attrs.UpsertString("bk.data.token", "token1")

		replacer := NewReplacer()
		replacer.replaceToken(config, record, attrs)

		assert.Equal(t, "tokena", record.Token.Original)
		value, ok := attrs.Get("bk.data.token")
		assert.True(t, ok)
		assert.Equal(t, "tokena", value.AsString())
	})

	t.Run("token_mapping: replace from original token when attribute empty", func(t *testing.T) {
		config := Config{
			Type:         typeTokenMapping,
			resourceKeys: []string{"bk.data.token"},
			replaceMapping: map[string]string{
				"token1": "tokena",
			},
		}

		record := &define.Record{
			Token: define.Token{Original: "token1"},
		}

		attrs := pcommon.NewMap()

		replacer := NewReplacer()
		replacer.replaceToken(config, record, attrs)

		assert.Equal(t, "tokena", record.Token.Original)
	})

	t.Run("token_mapping: no replacement when not found", func(t *testing.T) {
		config := Config{
			Type:         typeTokenMapping,
			resourceKeys: []string{"bk.data.token"},
			replaceMapping: map[string]string{
				"token1": "tokena",
			},
		}

		record := &define.Record{
			Token: define.Token{Original: "unknown_token"},
		}

		attrs := pcommon.NewMap()
		attrs.UpsertString("bk.data.token", "unknown_value")

		replacer := NewReplacer()
		replacer.replaceToken(config, record, attrs)

		assert.Equal(t, "unknown_token", record.Token.Original)
		value, ok := attrs.Get("bk.data.token")
		assert.True(t, ok)
		assert.Equal(t, "unknown_value", value.AsString())
	})

	t.Run("multiple resource keys: use first found", func(t *testing.T) {
		config := Config{
			Type:         typeTokenMapping,
			resourceKeys: []string{"key1", "key2", "key3"},
			replaceMapping: map[string]string{
				"token1": "tokena",
			},
		}

		record := &define.Record{
			Token: define.Token{Original: "original_token"},
		}

		attrs := pcommon.NewMap()
		attrs.UpsertString("key2", "token1")
		attrs.UpsertString("key3", "other_value")

		replacer := NewReplacer()
		replacer.replaceToken(config, record, attrs)

		assert.Equal(t, "tokena", record.Token.Original)
		value, ok := attrs.Get("key2")
		assert.True(t, ok)
		assert.Equal(t, "tokena", value.AsString())
	})
}
