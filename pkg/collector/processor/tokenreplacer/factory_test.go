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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/metacache"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/generator"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/mapstructure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/processor"
	"github.com/stretchr/testify/assert"
)

func TestFactory(t *testing.T) {
	content := `
processor:
  - name: "token_replacer/token_mapping"
    config:
      type: "token_mapping"
      resource_key: " bk.data.token "
      replace_list:
        - original: "token1"
          replace: "tokena"
        - original: "token2"
          replace: "tokenb"
`
	mainConf := processor.MustLoadConfigs(content)[0].Config

	customContent := `
processor:
  - name: "token_replacer/token_mapping"
    config:
      type: "token_mapping"
      resource_key: " bk.data.token, bk.data.another.token "
      replace_list:
        - original: "token1"
          replace: "tokenA"
        - original: "token2"
          replace: "tokenB"
`
	customConf := processor.MustLoadConfigs(customContent)[0].Config
	obj, err := NewFactory(mainConf, []processor.SubConfigProcessor{
		{
			Token: "token1",
			Type:  define.SubConfigFieldDefault,
			Config: processor.Config{
				Config: customConf,
			},
		},
	})
	factory := obj.(*tokenReplacer)
	assert.NoError(t, err)
	assert.Equal(t, mainConf, factory.MainConfig())

	var c1 Config
	assert.NoError(t, mapstructure.Decode(mainConf, &c1))
	(&c1).Clean()
	actualC1 := factory.configs.GetGlobal().(Config)
	assert.Equal(t, c1, actualC1)
	assert.Equal(t, []string{"bk.data.token"}, actualC1.resourceKeys)
	assert.Equal(t, map[string]string{"token1": "tokena", "token2": "tokenb"}, actualC1.replaceMapping)

	var c2 Config
	assert.NoError(t, mapstructure.Decode(customConf, &c2))
	(&c2).Clean()
	actualC2 := factory.configs.GetByToken("token1").(Config)
	assert.Equal(t, c2, actualC2)
	assert.Equal(t, []string{"bk.data.token", "bk.data.another.token"}, actualC2.resourceKeys)
	assert.Equal(t, map[string]string{"token1": "tokenA", "token2": "tokenB"}, actualC2.replaceMapping)

	assert.Equal(t, define.ProcessorTokenReplacer, factory.Name())
	assert.False(t, factory.IsDerived())
	assert.True(t, factory.IsPreCheck())
}

func makeTracesRecord(n int, resources map[string]string) ptrace.Traces {
	opts := define.TracesOptions{SpanCount: n}
	opts.Resources = resources
	return generator.NewTracesGenerator(opts).Generate()
}

func makeMetricsRecord(n int, resources map[string]string) pmetric.Metrics {
	opts := define.MetricsOptions{GaugeCount: n}
	opts.Resources = resources
	return generator.NewMetricsGenerator(opts).Generate()
}

func makeLogsRecord(n int, resources map[string]string) plog.Logs {
	opts := define.LogsOptions{LogName: "testlog", LogCount: n, LogLength: 10}
	opts.Resources = resources
	return generator.NewLogsGenerator(opts).Generate()
}

func TestTokenMappingAction(t *testing.T) {
	content := `
processor:
  - name: "token_replacer/token_mapping"
    config:
      type: "token_mapping"
      resource_key: " bk.data.token "
      replace_list:
        - original: "token1"
          replace: "tokena"
`

	t.Run("traces", func(t *testing.T) {
		factory := processor.MustCreateFactory(content, NewFactory)

		resources := map[string]string{
			"bk.data.token": "token1",
		}
		record := &define.Record{
			RecordType: define.RecordTraces,
			Data:       makeTracesRecord(1, resources),
		}

		_, err := factory.Process(record)
		assert.NoError(t, err)
		assert.Equal(t, "tokena", record.Token.Original)
	})

	t.Run("metrics", func(t *testing.T) {
		factory := processor.MustCreateFactory(content, NewFactory)

		resources := map[string]string{
			"bk.data.token": "token1",
		}
		record := &define.Record{
			RecordType: define.RecordMetrics,
			Data:       makeMetricsRecord(1, resources),
		}

		_, err := factory.Process(record)
		assert.NoError(t, err)
		assert.Equal(t, "tokena", record.Token.Original)
	})

	t.Run("logs", func(t *testing.T) {
		factory := processor.MustCreateFactory(content, NewFactory)

		resources := map[string]string{
			"bk.data.token": "token1",
		}
		record := &define.Record{
			RecordType: define.RecordLogs,
			Data:       makeLogsRecord(1, resources),
		}

		_, err := factory.Process(record)
		assert.NoError(t, err)
		assert.Equal(t, "tokena", record.Token.Original)
	})
}

func TestAppNameAction(t *testing.T) {
	const (
		token1 = "Ymtia2JrYmtia2JrYmtiaxUtdLzrldhHtlcjc1Cwfo1u99rVk5HGe8EjT761brGtKm3H4Ran78rWl85HwzfRgw=="
		token2 = "Ymtia2JrYmtia2JrYmtia/0ZJ3tXGU6OT2oEqyruVbvWr0kNl7AzgSWPsnVzNBYWRULf8XE/mtQBHLas+jYCrw=="
	)
	metacache.Set(token1, define.Token{
		AppName:  "app1",
		Original: token1,
	})
	metacache.Set(token2, define.Token{
		AppName:  "app2",
		Original: token2,
	})

	defer func() {
		metacache.Default = metacache.New()
	}()

	content := `
processor:
  - name: "token_replacer/app_name"
    config:
      type: "app_name"
      resource_key: "bk.data.token"
`

	t.Run("traces", func(t *testing.T) {
		factory := processor.MustCreateFactory(content, NewFactory)

		resources := map[string]string{
			"bk.data.token": "app1",
		}
		record := &define.Record{
			RecordType: define.RecordTraces,
			Data:       makeTracesRecord(1, resources),
		}

		_, err := factory.Process(record)
		assert.NoError(t, err)
		assert.Equal(t, token1, record.Token.Original)
	})

	t.Run("metrics", func(t *testing.T) {
		factory := processor.MustCreateFactory(content, NewFactory)

		resources := map[string]string{
			"bk.data.token": "app1",
		}
		record := &define.Record{
			RecordType: define.RecordMetrics,
			Data:       makeMetricsRecord(1, resources),
		}

		_, err := factory.Process(record)
		assert.NoError(t, err)
		assert.Equal(t, token1, record.Token.Original)
	})

	t.Run("logs", func(t *testing.T) {
		factory := processor.MustCreateFactory(content, NewFactory)

		resources := map[string]string{
			"bk.data.token": "app1",
		}
		record := &define.Record{
			RecordType: define.RecordLogs,
			Data:       makeLogsRecord(1, resources),
		}

		_, err := factory.Process(record)
		assert.NoError(t, err)
		assert.Equal(t, token1, record.Token.Original)
	})
}
