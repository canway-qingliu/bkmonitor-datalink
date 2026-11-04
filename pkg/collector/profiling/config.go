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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

// keyCollectorPrefix 是主配置里 collector 自配置段的根键。libbeat 会剥掉这一层再交给
// collector，但 subconfig 文件是直接读原始 YAML 的，不做剥壳。
const keyCollectorPrefix = "bk-collector"

// Config 描述了 profiling 采集的完整参数
// Disabled 与 Kingeye 下发的总开关一致：true 表示不采集，false 表示允许采集。
// 无论是否禁用，接口均返回 200 并下发其余参数，由客户端决定是否采集。
type Config struct {
	Disabled bool         `config:"disabled" json:"disabled" mapstructure:"disabled"`
	CPU      CPUConfig    `config:"cpu" json:"cpu"`
	Memory   MemoryConfig `config:"memory" json:"memory"`
}

// CPUConfig 描述 CPU profiling 参数，sampling_frequency 为采样频率（如 "99Hz"）。
// 三个参数均按不透明字符串保留 YAML 原文并通过 JSON 下发，由客户端解析。
type CPUConfig struct {
	Enabled           bool   `config:"enabled" json:"enabled" mapstructure:"enabled"`
	CollectionPeriod  string `config:"collection_period" json:"collection_period" mapstructure:"collection_period"`
	CollectionWindow  string `config:"collection_window" json:"collection_window" mapstructure:"collection_window"`
	SamplingFrequency string `config:"sampling_frequency" json:"sampling_frequency" mapstructure:"sampling_frequency"`
}

// MemoryConfig 描述内存 profiling 参数，allocation_threshold 为分配阈值（如 "1MB"）。
// 三个参数均按不透明字符串保留 YAML 原文并通过 JSON 下发，由客户端解析。
type MemoryConfig struct {
	Enabled             bool   `config:"enabled" json:"enabled" mapstructure:"enabled"`
	CollectionPeriod    string `config:"collection_period" json:"collection_period" mapstructure:"collection_period"`
	CollectionWindow    string `config:"collection_window" json:"collection_window" mapstructure:"collection_window"`
	AllocationThreshold string `config:"allocation_threshold" json:"allocation_threshold" mapstructure:"allocation_threshold"`
}

// LoadConfig 解析主 profiling 块，并合并 subconfig 文件中的 per-token 覆盖
// 默认配置通过 SetGlobal 落入 TierConfig；每条 type=subconfig 的文件以 token 为键覆盖默认
func LoadConfig(conf *confengine.Config) (*confengine.TierConfig, error) {
	// 主配置未声明总开关时默认不采集；应用子配置可用 disabled: false 显式开启。
	def := Config{Disabled: true}
	if err := conf.UnpackChild(define.ConfigFieldProfiling, &def); err != nil {
		return nil, err
	}

	var apmConf define.ApmConfig
	if conf.Has(define.ConfigFieldApmConfig) {
		if err := conf.UnpackChild(define.ConfigFieldApmConfig, &apmConf); err != nil {
			logger.Warnf("profiling: failed to unpack apm config, no per-token overrides will be applied: %v", err)
			apmConf = define.ApmConfig{}
		}
	}

	tier := confengine.NewTierConfig()
	tier.SetGlobal(&def)

	type subConfig struct {
		Type  string `config:"type"`
		Token string `config:"token"`
	}

	for _, sc := range confengine.LoadConfigPatterns(apmConf.Patterns) {
		var sub subConfig
		if err := sc.Unpack(&sub); err != nil {
			continue
		}
		if sub.Type != define.ConfigTypeSubConfig || sub.Token == "" {
			continue
		}
		// profiling 段必须平铺在 subconfig 根层级：主配置的 bk-collector 段由 libbeat 剥去后
		// 才交给 collector，而 subconfig 是直接读原始文件的，嵌在 bk-collector 之下取不到
		if !sc.Has("profiling") {
			if sc.Has(keyCollectorPrefix + "." + "profiling") {
				logger.Warnf("profiling: subconfig for token %s nests profiling under '%s', "+
					"but it must be at the root level; per-token override skipped", sub.Token, keyCollectorPrefix)
			}
			continue
		}

		var override Config
		if err := sc.UnpackChild("profiling", &override); err != nil {
			logger.Warnf("profiling: failed to decode override for token %s: %v", sub.Token, err)
			continue
		}
		// 总开关缺省继承主配置；显式 disabled: false 必须覆盖主配置的关闭状态。
		if child, err := sc.Child("profiling"); err == nil && !child.Has("disabled") {
			override.Disabled = def.Disabled
		}
		tier.Set(sub.Token, define.SubConfigFieldDefault, "", &override)
	}

	return tier, nil
}
