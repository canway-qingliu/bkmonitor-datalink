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
    sampling_frequency: "512KB"
`

func resetGlobal() {
	mu.Lock()
	globalTier = nil
	mu.Unlock()
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

	var got Config
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "99Hz", got.CPU.SamplingFrequency)
	assert.Equal(t, "512KB", got.Memory.SamplingFrequency)
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
    sampling_frequency: "1MB"
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
    sampling_frequency: "2MB"
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
		token          string
		cpuEnabled     bool
		cpuSampling    string
		memoryEnabled  bool
		memorySampling string
	}{
		{
			token:          "token-a",
			cpuEnabled:     true,
			cpuSampling:    "199Hz",
			memoryEnabled:  false,
			memorySampling: "1MB",
		},
		{
			token:          "token-b",
			cpuEnabled:     false,
			cpuSampling:    "299Hz",
			memoryEnabled:  true,
			memorySampling: "2MB",
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
			var got Config
			assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
			assert.Equal(t, tt.cpuEnabled, got.CPU.Enabled)
			assert.Equal(t, tt.cpuSampling, got.CPU.SamplingFrequency)
			assert.Equal(t, tt.memoryEnabled, got.Memory.Enabled)
			assert.Equal(t, tt.memorySampling, got.Memory.SamplingFrequency)
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
