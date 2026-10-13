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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/json"
)

const testDefaultYAML = `
profiling:
  enabled: true
  cpu:
    enabled: true
    collection_period: "10m"
    collection_window: "30s"
    sampling_frequency: "99Hz"
  memory:
    enabled: true
    collection_period: "15m"
    collection_window: "1m"
    sampling_frequency: "512KB"
`

// 存量配置的形态：只有 cpu/memory，没有总开关
const testDefaultYAMLWithoutEnabled = `
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
    sampling_frequency: "512KB"
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
	assert.True(t, def.Enabled)
	assert.True(t, def.CPU.Enabled)
	assert.Equal(t, 10*time.Minute, def.CPU.CollectionPeriod)
	assert.Equal(t, 30*time.Second, def.CPU.CollectionWindow)
	assert.Equal(t, "99Hz", def.CPU.SamplingFrequency)

	assert.True(t, def.Memory.Enabled)
	assert.Equal(t, 15*time.Minute, def.Memory.CollectionPeriod)
	assert.Equal(t, time.Minute, def.Memory.CollectionWindow)
	assert.Equal(t, "512KB", def.Memory.SamplingFrequency)
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
    sampling_frequency: "1MB"
`
	conf2 := newConf(t, updated)
	assert.NoError(t, Reload(conf2))
	defer resetGlobal()

	cfg := GetConfig("any")
	assert.NotNil(t, cfg)
	assert.False(t, cfg.CPU.Enabled)
	assert.Equal(t, 20*time.Minute, cfg.CPU.CollectionPeriod)
	assert.Equal(t, 45*time.Second, cfg.CPU.CollectionWindow)
	assert.Equal(t, "199Hz", cfg.CPU.SamplingFrequency)
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
    sampling_frequency: "512KB"
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
    sampling_frequency: "512KB"
`
	conf := newConf(t, yaml)
	assert.NoError(t, New(conf))
	defer resetGlobal()

	// 命中 subconfig 覆盖：override 整体替换 default（与 pproftranslator 一致）
	cfgA := GetConfig("token-a")
	assert.NotNil(t, cfgA)
	assert.False(t, cfgA.CPU.Enabled)
	assert.Equal(t, 20*time.Minute, cfgA.CPU.CollectionPeriod)
	assert.Equal(t, 45*time.Second, cfgA.CPU.CollectionWindow)
	assert.Equal(t, "199Hz", cfgA.CPU.SamplingFrequency)
	// 未命中时回退默认
	cfgB := GetConfig("token-b")
	assert.NotNil(t, cfgB)
	assert.True(t, cfgB.CPU.Enabled)
	assert.Equal(t, "99Hz", cfgB.CPU.SamplingFrequency)
}
// 回归：主配置缺少 profiling 块时，Reload 不应报错，且视为无配置（GetConfig 返回 nil）
func TestReload_MissingBlockTreatedAsNoConfig(t *testing.T) {
	defer resetGlobal()

	conf := newConf(t, "apm:\n  patterns: []\n")
	assert.NoError(t, Reload(conf))
	assert.Nil(t, GetConfig("any-token"))
}

// 回归：enabled 为 false 时仍正常加载配置，接口据此告知客户端不要采集
func TestReload_EnabledFalseStillLoads(t *testing.T) {
	defer resetGlobal()

	conf := newConf(t, testDefaultYAMLWithoutEnabled)
	assert.NoError(t, Reload(conf))

	cfg := GetConfig("any-token")
	assert.NotNil(t, cfg)
	assert.False(t, cfg.Enabled)
	assert.Equal(t, "99Hz", cfg.CPU.SamplingFrequency)
}

// 总开关未声明时缺省为 false：存量配置不写 enabled，拉到的就是 enabled=false，由客户端自行决定不采
func TestTotalSwitch_DefaultsToFalseWhenAbsent(t *testing.T) {
	conf := newConf(t, testDefaultYAMLWithoutEnabled)
	tier, err := LoadConfig(conf)
	assert.NoError(t, err)

	def := tier.GetGlobal().(*Config)
	assert.False(t, def.Enabled)
}

// 总开关为 false 时接口仍返回 200：它只告知客户端，不阻断配置下发（与 disabled 的 404 语义不同）
func TestTotalSwitch_FalseStillServesConfig(t *testing.T) {
	defer resetGlobal()

	conf := newConf(t, testDefaultYAMLWithoutEnabled)
	assert.NoError(t, New(conf))

	req := httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
	w := httptest.NewRecorder()
	handleProfilingConfig(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var got Config
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.False(t, got.Enabled)
	// 总开关关闭不影响其余参数下发
	assert.Equal(t, "99Hz", got.CPU.SamplingFrequency)
}

// subconfig 未声明 enabled 时继承主配置，避免调整采样参数顺带把总开关抹成 false
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
    sampling_frequency: "512KB"
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
	assert.True(t, cfg.Enabled)
	assert.Equal(t, "199Hz", cfg.CPU.SamplingFrequency)
}

// subconfig 显式声明 enabled 时以子配置为准
func TestTotalSwitch_SubconfigOverridesWhenPresent(t *testing.T) {
	defer resetGlobal()

	dir := t.TempDir()
	subPath := filepath.Join(dir, "sub.yml")
	subContent := `
type: "subconfig"
token: "token-a"
profiling:
  enabled: false
  cpu:
    enabled: true
    collection_period: "20m"
    collection_window: "45s"
    sampling_frequency: "199Hz"
  memory:
    enabled: true
    collection_period: "15m"
    collection_window: "1m"
    sampling_frequency: "512KB"
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
	assert.False(t, cfg.Enabled)

	// 未命中 subconfig 的 token 仍取主配置
	cfgB := GetConfig("token-b")
	assert.NotNil(t, cfgB)
	assert.True(t, cfgB.Enabled)
}
