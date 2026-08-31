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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/processor"
)

func TestFactory(t *testing.T) {
	mainConf := mustLoadProcessorConfig(`
processor:
  - name: "skywalking_processor/default"
    config:
      meter_data:
        enabled: false
`)
	customConf := mustLoadProcessorConfig(`
processor:
  - name: "skywalking_processor/default"
    config:
      meter_data:
        enabled: true
        rules:
          - types: ["*"]
            metrics:
              activeConnections: activeConnections
`)

	obj, err := NewFactory(mainConf, []processor.SubConfigProcessor{
		{
			Token:  "token1",
			Type:   define.SubConfigFieldDefault,
			Config: processor.Config{Config: customConf},
		},
	})
	require.NoError(t, err)
	factory := obj.(*skywalkingProcessor)
	assert.Equal(t, define.ProcessorSkywalking, factory.Name())
	assert.False(t, factory.IsDerived())
	assert.False(t, factory.IsPreCheck())
	assert.Equal(t, mainConf, factory.MainConfig())
	assert.False(t, factory.configs.GetGlobal().(Config).MeterData.Enabled)
	assert.True(t, factory.configs.GetByToken("token1").(Config).MeterData.Enabled)
	assert.False(t, factory.configs.GetByToken("unknown").(Config).MeterData.Enabled)

	factory.Reload(mainConf, nil)
	assert.Equal(t, mainConf, factory.MainConfig())
}

func TestFactoryRejectsInvalidMainConfig(t *testing.T) {
	t.Run("enabled action requires rules", func(t *testing.T) {
		mainConf := mustLoadProcessorConfig(`
processor:
  - name: "skywalking_processor/default"
    config:
      meter_data:
        enabled: true
`)

		obj, err := NewFactory(mainConf, nil)
		assert.ErrorContains(t, err, "no rules specified")
		assert.Nil(t, obj)
	})
}

func mustLoadProcessorConfig(content string) map[string]any {
	return processor.MustLoadConfigs(content)[0].Config
}
