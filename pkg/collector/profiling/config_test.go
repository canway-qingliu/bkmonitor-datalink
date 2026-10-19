// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package profiling

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/json"
)

const testDefaultYAML = `
profiling:
  disabled: false
  cpu:
    enabled: true
    collection_period: "10m"
    collection_window: "30s"
    sampling_frequency: "99Hz"
  memory:
    enabled: true
    collection_period: "15m"
    collection_window: "1m"
    allocation_threshold: "512KB"
`

// 存量配置的形态：只有 cpu/memory，没有总开关
const testDefaultYAMLWithoutDisabled = `
profiling:
  cpu:
    enabled: true
    collection_period: "10m"
    collection_window: "30s"
    sampling_frequency: "99Hz"
  memory:
    enabled: true
    collection_period: "15m"
    collection_window: "1m"
    allocation_threshold: "512KB"
`

func newConf(t *testing.T, content string) *confengine.Config {
	t.Helper()
	conf, err := confengine.LoadConfigContent(content)
	assert.NoError(t, err)
	return conf
}

func TestLoadConfig_Default(t *testing.T) {
	conf := newConf(t, testDefaultYAML)
	tier, err := LoadConfig(conf)
	assert.NoError(t, err)
	assert.NotNil(t, tier)

	def := tier.GetGlobal().(*Config)
	assert.False(t, def.Disabled)
	assert.True(t, def.CPU.Enabled)
	assert.Equal(t, "10m", def.CPU.CollectionPeriod)
	assert.Equal(t, "30s", def.CPU.CollectionWindow)
	assert.Equal(t, "99Hz", def.CPU.SamplingFrequency)

	assert.True(t, def.Memory.Enabled)
	assert.Equal(t, "15m", def.Memory.CollectionPeriod)
	assert.Equal(t, "1m", def.Memory.CollectionWindow)
	assert.Equal(t, "512KB", def.Memory.AllocationThreshold)
}

func TestGetConfig_FallsBackToDefault(t *testing.T) {
	conf := newConf(t, testDefaultYAML)
	err := New(conf)
	assert.NoError(t, err)
	defer resetGlobal()

	cfg := GetConfig("unknown-token")
	assert.NotNil(t, cfg)
	assert.Equal(t, "99Hz", cfg.CPU.SamplingFrequency)
}

func TestReload(t *testing.T) {
	conf := newConf(t, testDefaultYAML)
	assert.NoError(t, New(conf))

	updated := `
profiling:
  cpu:
    enabled: false
    collection_period: "20m"
    collection_window: "45s"
    sampling_frequency: "199Hz"
  memory:
    enabled: false
    collection_period: "30m"
    collection_window: "2m"
    allocation_threshold: "1MB"
`
	conf2 := newConf(t, updated)
	assert.NoError(t, Reload(conf2))
	defer resetGlobal()

	cfg := GetConfig("any")
	assert.NotNil(t, cfg)
	assert.False(t, cfg.CPU.Enabled)
	assert.Equal(t, "20m", cfg.CPU.CollectionPeriod)
	assert.Equal(t, "45s", cfg.CPU.CollectionWindow)
	assert.Equal(t, "199Hz", cfg.CPU.SamplingFrequency)
	assert.False(t, cfg.Memory.Enabled)
	assert.Equal(t, "30m", cfg.Memory.CollectionPeriod)
	assert.Equal(t, "2m", cfg.Memory.CollectionWindow)
	assert.Equal(t, "1MB", cfg.Memory.AllocationThreshold)
}

func TestPerTokenOverrideFromSubconfig(t *testing.T) {
	dir := t.TempDir()
	subPath := filepath.Join(dir, "sub.yml")
	subContent := `
type: "subconfig"
token: "token-a"
profiling:
  cpu:
    enabled: false
    collection_period: "20m"
    collection_window: "45s"
    sampling_frequency: "199Hz"
  memory:
    enabled: true
    collection_period: "15m"
    collection_window: "1m"
    allocation_threshold: "1MB"
`
	assert.NoError(t, os.WriteFile(subPath, []byte(subContent), 0o644))

	pattern := filepath.Join(dir, "*.yml")
	yaml := `
apm:
  patterns:
    - "` + pattern + `"
profiling:
  cpu:
    enabled: true
    collection_period: "10m"
    collection_window: "30s"
    sampling_frequency: "99Hz"
  memory:
    enabled: true
    collection_period: "15m"
    collection_window: "1m"
    allocation_threshold: "512KB"
`
	conf := newConf(t, yaml)
	assert.NoError(t, New(conf))
	defer resetGlobal()

	// 命中 subconfig 覆盖：override 整体替换 default（与 pproftranslator 一致）
	cfgA := GetConfig("token-a")
	assert.NotNil(t, cfgA)
	assert.False(t, cfgA.CPU.Enabled)
	assert.Equal(t, "20m", cfgA.CPU.CollectionPeriod)
	assert.Equal(t, "45s", cfgA.CPU.CollectionWindow)
	assert.Equal(t, "199Hz", cfgA.CPU.SamplingFrequency)
	assert.Equal(t, "1MB", cfgA.Memory.AllocationThreshold)
	// 未命中时回退默认
	cfgB := GetConfig("token-b")
	assert.NotNil(t, cfgB)
	assert.True(t, cfgB.CPU.Enabled)
	assert.Equal(t, "99Hz", cfgB.CPU.SamplingFrequency)
	assert.Equal(t, "512KB", cfgB.Memory.AllocationThreshold)
}

// 回归：主配置缺少 profiling 块时，Reload 不应报错，且视为无配置（GetConfig 返回 nil）
func TestReload_MissingBlockTreatedAsNoConfig(t *testing.T) {
	defer resetGlobal()

	conf := newConf(t, "apm:\n  patterns: []\n")
	assert.NoError(t, Reload(conf))
	assert.Nil(t, GetConfig("any-token"))
}

// 回归：disabled 为 true 时仍正常加载配置，接口据此告知客户端不要采集
func TestReload_DisabledTrueStillLoads(t *testing.T) {
	defer resetGlobal()

	conf := newConf(t, strings.Replace(testDefaultYAML, "disabled: false", "disabled: true", 1))
	assert.NoError(t, Reload(conf))

	cfg := GetConfig("any-token")
	assert.NotNil(t, cfg)
	assert.True(t, cfg.Disabled)
	assert.Equal(t, "99Hz", cfg.CPU.SamplingFrequency)
}

// 总开关未声明时缺省为 disabled=true，由客户端停止采集。
func TestTotalSwitch_DefaultsToDisabledWhenAbsent(t *testing.T) {
	conf := newConf(t, testDefaultYAMLWithoutDisabled)
	tier, err := LoadConfig(conf)
	assert.NoError(t, err)

	def := tier.GetGlobal().(*Config)
	assert.True(t, def.Disabled)
}

// disabled 为 true 时接口仍返回 200：它只告知客户端，不阻断配置下发。
func TestTotalSwitch_DisabledStillServesConfig(t *testing.T) {
	defer resetGlobal()

	conf := newConf(t, strings.Replace(testDefaultYAML, "disabled: false", "disabled: true", 1))
	assert.NoError(t, New(conf))

	req := httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
	w := httptest.NewRecorder()
	handleProfilingConfig(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"disabled":true`)

	var got Config
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.True(t, got.Disabled)
	// 总开关关闭不影响其余参数下发
	assert.Equal(t, "99Hz", got.CPU.SamplingFrequency)
}

// subconfig 未声明 disabled 时继承主配置，避免调整采样参数改变总开关。
func TestTotalSwitch_SubconfigInheritsWhenAbsent(t *testing.T) {
	defer resetGlobal()

	dir := t.TempDir()
	subPath := filepath.Join(dir, "sub.yml")
	subContent := `
type: "subconfig"
token: "token-a"
profiling:
  cpu:
    enabled: true
    collection_period: "20m"
    collection_window: "45s"
    sampling_frequency: "199Hz"
  memory:
    enabled: true
    collection_period: "15m"
    collection_window: "1m"
    allocation_threshold: "512KB"
`
	assert.NoError(t, os.WriteFile(subPath, []byte(subContent), 0o644))

	baseYAML := `
apm:
  patterns:
    - "` + filepath.Join(dir, "*.yml") + `"
`
	tests := []struct {
		name     string
		config   string
		disabled bool
	}{
		{name: "enabled", config: testDefaultYAML, disabled: false},
		{name: "disabled", config: strings.Replace(testDefaultYAML, "disabled: false", "disabled: true", 1), disabled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := newConf(t, baseYAML+tt.config)
			assert.NoError(t, New(conf))

			cfg := GetConfig("token-a")
			assert.NotNil(t, cfg)
			assert.Equal(t, tt.disabled, cfg.Disabled)
			assert.Equal(t, "199Hz", cfg.CPU.SamplingFrequency)
		})
	}
}

// subconfig 显式声明 disabled 时以子配置为准。
func TestTotalSwitch_SubconfigOverridesWhenPresent(t *testing.T) {
	defer resetGlobal()

	dir := t.TempDir()
	subPath := filepath.Join(dir, "sub.yml")
	subContent := `
type: "subconfig"
token: "token-a"
profiling:
  disabled: true
  cpu:
    enabled: true
    collection_period: "20m"
    collection_window: "45s"
    sampling_frequency: "199Hz"
  memory:
    enabled: true
    collection_period: "15m"
    collection_window: "1m"
    allocation_threshold: "512KB"
`
	assert.NoError(t, os.WriteFile(subPath, []byte(subContent), 0o644))

	yaml := `
apm:
  patterns:
    - "` + filepath.Join(dir, "*.yml") + `"
` + testDefaultYAML
	conf := newConf(t, yaml)
	assert.NoError(t, New(conf))

	cfg := GetConfig("token-a")
	assert.NotNil(t, cfg)
	assert.True(t, cfg.Disabled)

	// 未命中 subconfig 的 token 仍取主配置
	cfgB := GetConfig("token-b")
	assert.NotNil(t, cfgB)
	assert.False(t, cfgB.Disabled)
}
