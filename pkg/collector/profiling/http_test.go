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

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/json"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/receiver"
)

const defaultConfigYAML = `
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

func resetGlobal() {
	mu.Lock()
	globalTier = nil
	mu.Unlock()
}

func TestReadyRegistersConfigRoute(t *testing.T) {
	Ready()

	var match mux.RouteMatch
	assert.True(t, receiver.RecvHttpRouter().Match(
		httptest.NewRequest(http.MethodGet, "/profiling/config", nil), &match))
	assert.False(t, receiver.RecvHttpRouter().Match(
		httptest.NewRequest(http.MethodPost, "/profiling/config", nil), &mux.RouteMatch{}))
}

// Kingeye 应用下发子配置时携带的 token
const kingeyeCPUOnlyToken = "kingeye-cpu-only"

// Kingeye 应用下发的子配置：只开启 CPU 采集，内存段整体缺省
const kingeyeCPUOnlySubConfigYAML = `
type: "subconfig"
token: "` + kingeyeCPUOnlyToken + `"
profiling:
  disabled: false
  cpu:
    enabled: true
    collection_period: "5m"
    collection_window: "30s"
    sampling_frequency: "99Hz"
`

// Kingeye 应用下发的子配置：CPU 与内存采集都开启
const kingeyeAllocationThresholdSubConfigYAML = `
type: "subconfig"
token: "kingeye-allocation-threshold"
profiling:
  disabled: false
  cpu:
    enabled: true
    collection_period: "5m"
    collection_window: "30s"
    sampling_frequency: "99Hz"
  memory:
    enabled: true
    collection_period: "15m"
    collection_window: "1m"
    allocation_threshold: "1MB"
`

// writeSubconfig 把子配置内容写入临时目录，返回匹配该文件的 glob 模式
func writeSubconfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "subconfig.yml"), []byte(content), 0o644))
	return filepath.Join(dir, "*.yml")
}

func TestHandler_DefaultConfig(t *testing.T) {
	defer resetGlobal()

	conf, err := confengine.LoadConfigContent(defaultConfigYAML)
	assert.NoError(t, err)
	assert.NoError(t, New(conf))

	req := httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
	w := httptest.NewRecorder()
	handleProfilingConfig(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
	assert.JSONEq(t, `{
		"level": "main",
		"disabled": true,
		"cpu": {"enabled": true, "collection_period": "10m", "collection_window": "30s", "sampling_frequency": "99Hz"},
		"memory": {"enabled": true, "collection_period": "15m", "collection_window": "1m", "allocation_threshold": "512KB"}
	}`, w.Body.String())

	var got Config
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "99Hz", got.CPU.SamplingFrequency)
	assert.Equal(t, "512KB", got.Memory.AllocationThreshold)
}

func TestHandler_WithToken(t *testing.T) {
	defer resetGlobal()

	conf, err := confengine.LoadConfigContent(defaultConfigYAML)
	assert.NoError(t, err)
	assert.NoError(t, New(conf))

	req := httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
	req.Header.Set("X-BK-TOKEN", "any-token")
	w := httptest.NewRecorder()
	handleProfilingConfig(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, `"sampling_frequency":"99Hz"`)
}

func TestHandler_PreservesParameterStrings(t *testing.T) {
	defer resetGlobal()

	conf, err := confengine.LoadConfigContent(`
profiling:
  disabled: false
  cpu:
    enabled: true
    collection_period: "300s"
    collection_window: "1.50s"
    sampling_frequency: "1kHz"
  memory:
    enabled: true
    collection_period: "0.5h"
    collection_window: "1500ms"
    allocation_threshold: "1KiB"
`)
	assert.NoError(t, err)
	assert.NoError(t, New(conf))

	req := httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
	w := httptest.NewRecorder()
	handleProfilingConfig(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	// 按客户端收到的 JSON 校验字符串类型与原文，不能通过 Config 往返掩盖单位转换。
	assert.JSONEq(t, `{
		"level": "main",
		"disabled": false,
		"cpu": {"enabled": true, "collection_period": "300s", "collection_window": "1.50s", "sampling_frequency": "1kHz"},
		"memory": {"enabled": true, "collection_period": "0.5h", "collection_window": "1500ms", "allocation_threshold": "1KiB"}
	}`, w.Body.String())
}

func TestHandler_DifferentTokensReturnDifferentConfigs(t *testing.T) {
	defer resetGlobal()

	dir := t.TempDir()
	subconfigs := map[string]string{
		"token-a": `
type: "subconfig"
token: "token-a"
profiling:
  cpu:
    enabled: true
    collection_period: "5m"
    collection_window: "10s"
    sampling_frequency: "199Hz"
  memory:
    enabled: false
    collection_period: "30m"
    collection_window: "2m"
    allocation_threshold: "1MB"
`,
		"token-b": `
type: "subconfig"
token: "token-b"
profiling:
  cpu:
    enabled: false
    collection_period: "20m"
    collection_window: "45s"
    sampling_frequency: "299Hz"
  memory:
    enabled: true
    collection_period: "40m"
    collection_window: "3m"
    allocation_threshold: "2MB"
`,
	}
	for token, content := range subconfigs {
		path := filepath.Join(dir, token+".yml")
		assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}

	conf, err := confengine.LoadConfigContent(`
apm:
  patterns:
    - "` + filepath.Join(dir, "*.yml") + `"
` + defaultConfigYAML)
	assert.NoError(t, err)
	assert.NoError(t, New(conf))

	tests := []struct {
		token           string
		cpuEnabled      bool
		cpuPeriod       string
		cpuWindow       string
		cpuSampling     string
		memoryEnabled   bool
		memoryPeriod    string
		memoryWindow    string
		memoryThreshold string
	}{
		{
			token:           "token-a",
			cpuEnabled:      true,
			cpuPeriod:       "5m",
			cpuWindow:       "10s",
			cpuSampling:     "199Hz",
			memoryEnabled:   false,
			memoryPeriod:    "30m",
			memoryWindow:    "2m",
			memoryThreshold: "1MB",
		},
		{
			token:           "token-b",
			cpuEnabled:      false,
			cpuPeriod:       "20m",
			cpuWindow:       "45s",
			cpuSampling:     "299Hz",
			memoryEnabled:   true,
			memoryPeriod:    "40m",
			memoryWindow:    "3m",
			memoryThreshold: "2MB",
		},
	}

	configs := make(map[string]Config, len(tests))
	for _, tt := range tests {
		t.Run(tt.token, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
			req.Header.Set("X-BK-TOKEN", tt.token)
			w := httptest.NewRecorder()
			handleProfilingConfig(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			body := w.Body.String()
			assert.Contains(t, body, `"collection_period":"`+tt.cpuPeriod+`"`)
			assert.Contains(t, body, `"collection_window":"`+tt.cpuWindow+`"`)
			assert.Contains(t, body, `"collection_period":"`+tt.memoryPeriod+`"`)
			assert.Contains(t, body, `"collection_window":"`+tt.memoryWindow+`"`)
			assert.Contains(t, body, `"allocation_threshold":"`+tt.memoryThreshold+`"`)
			var got Config
			assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
			assert.Equal(t, tt.cpuEnabled, got.CPU.Enabled)
			assert.Equal(t, tt.cpuSampling, got.CPU.SamplingFrequency)
			assert.Equal(t, tt.memoryEnabled, got.Memory.Enabled)
			assert.Equal(t, tt.memoryThreshold, got.Memory.AllocationThreshold)
			configs[tt.token] = got
		})
	}

	assert.NotEqual(t, configs["token-a"], configs["token-b"])
	assert.NotEqual(t, configs["token-a"].CPU.SamplingFrequency, configs["token-b"].CPU.SamplingFrequency)
}

func TestHandler_NoConfig(t *testing.T) {
	defer resetGlobal()

	// profiling 块整体缺省时视为无配置：New 不报错，handler 返回 no profiling config
	conf, err := confengine.LoadConfigContent(`
apm:
  patterns: []
`)
	assert.NoError(t, err)
	assert.NoError(t, New(conf))
	assert.Nil(t, GetConfig("any-token"))

	req := httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
	w := httptest.NewRecorder()
	handleProfilingConfig(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "no profiling config"))
}

func TestHandler_KingeyeCPUOnly(t *testing.T) {
	defer resetGlobal()

	pattern := writeSubconfig(t, kingeyeCPUOnlySubConfigYAML)
	conf, err := confengine.LoadConfigContent(`
profiling:
  disabled: true
apm:
  patterns:
    - "` + pattern + `"
`)
	require.NoError(t, err)
	require.NoError(t, New(conf))

	req := httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
	req.Header.Set("X-BK-TOKEN", kingeyeCPUOnlyToken)
	w := httptest.NewRecorder()
	handleProfilingConfig(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{
		"level": "app",
		"disabled": false,
		"cpu": {"enabled": true, "collection_period": "5m", "collection_window": "30s", "sampling_frequency": "99Hz"},
		"memory": {"enabled": false, "collection_period": "", "collection_window": "", "allocation_threshold": ""}
	}`, w.Body.String())

	// 应用子配置只开启自己的采集，其他 token 仍使用主配置的关闭状态。
	req = httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
	req.Header.Set("X-BK-TOKEN", "other-token")
	w = httptest.NewRecorder()
	handleProfilingConfig(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"disabled":true`)
}

func TestHandler_KingeyeAllocationThreshold(t *testing.T) {
	defer resetGlobal()

	pattern := writeSubconfig(t, kingeyeAllocationThresholdSubConfigYAML)
	conf, err := confengine.LoadConfigContent(`
profiling:
  disabled: true
apm:
  patterns:
    - "` + pattern + `"
`)
	require.NoError(t, err)
	require.NoError(t, New(conf))

	req := httptest.NewRequest(http.MethodGet, "/profiling/config", nil)
	req.Header.Set("X-BK-TOKEN", "kingeye-allocation-threshold")
	w := httptest.NewRecorder()
	handleProfilingConfig(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	// 直接校验响应键名，确保 CPU 频率和内存分配阈值分别下发，且没有多余的跨类型字段。
	assert.JSONEq(t, `{
		"level": "app",
		"disabled": false,
		"cpu": {"enabled": true, "collection_period": "5m", "collection_window": "30s", "sampling_frequency": "99Hz"},
		"memory": {"enabled": true, "collection_period": "15m", "collection_window": "1m", "allocation_threshold": "1MB"}
	}`, w.Body.String())
}
