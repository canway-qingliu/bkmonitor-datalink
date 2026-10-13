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
	"errors"
	"sync"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
)

var (
	errNoProfilingConfig = errors.New("no profiling config")
)

var (
	mu         sync.RWMutex
	globalTier *confengine.TierConfig
)

// New 初始化 profiling 全局状态
func New(conf *confengine.Config) error {
	return Reload(conf)
}

// Reload 热加载 profiling 配置
func Reload(conf *confengine.Config) error {
	// profiling 块整体缺省视为无配置：LoadConfig 的 UnpackChild 会因缺字段直接失败，
	// 这里兜底后 globalTier 保持 nil，接口返回 no profiling config
	if !conf.Has(define.ConfigFieldProfiling) {
		mu.Lock()
		globalTier = nil
		mu.Unlock()
		return nil
	}

	tier, err := LoadConfig(conf)
	if err != nil {
		return err
	}

	mu.Lock()
	globalTier = tier
	mu.Unlock()
	return nil
}

// GetConfig 返回指定 token 的生效 profiling 配置；未命中 per-token 覆盖时返回默认配置
func GetConfig(token string) *Config {
	mu.RLock()
	defer mu.RUnlock()
	if globalTier == nil {
		return nil
	}

	raw := globalTier.Get(token, "", "")
	if raw == nil {
		return nil
	}

	cfg, ok := raw.(*Config)
	if !ok {
		return nil
	}
	return cfg
}
