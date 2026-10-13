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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

// keyCollectorPrefix 是主配置里 collector 自配置段的根键。libbeat 会剥掉这一层再交给
// collector，但 subconfig 文件是直接读原始 YAML 的，不做剥壳。
const keyCollectorPrefix = "bk-collector"

// Config 描述了 profiling 采集的完整参数
// Enabled 是 profiling 总开关，随接口返回给客户端，由客户端自行决定是否采集；
// 它为 false 时接口照常返回 200 并完整下发其余参数，不阻断配置获取
type Config struct {
	Enabled bool          `config:"enabled" json:"enabled" mapstructure:"enabled"`
	CPU     ProfileConfig `config:"cpu" json:"cpu"`
	Memory  ProfileConfig `config:"memory" json:"memory"`
}

// ProfileConfig 描述了单类 profiling 的采集参数；sampling_frequency 的单位由 CPU 或 Memory 类型决定。
type ProfileConfig struct {
	Enabled           bool          `config:"enabled" json:"enabled" mapstructure:"enabled"`
	CollectionPeriod  time.Duration `config:"collection_period" json:"collection_period" mapstructure:"collection_period"`
	CollectionWindow  time.Duration `config:"collection_window" json:"collection_window" mapstructure:"collection_window"`
	SamplingFrequency string        `config:"sampling_frequency" json:"sampling_frequency" mapstructure:"sampling_frequency"`
}

// LoadConfig 解析主 profiling 块，并合并 subconfig 文件中的 per-token 覆盖
// 默认配置通过 SetGlobal 落入 TierConfig；每条 type=subconfig 的文件以 token 为键覆盖默认
func LoadConfig(conf *confengine.Config) (*confengine.TierConfig, error) {
	var def Config
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
		// 总开关缺省继承主配置：override 虽整体替换主 profiling 块，但 enabled 未显式声明时
		// 不应把主配置的开关值抹成 false，否则单 token 的采样参数调整会顺带关掉该应用的 profiling
		if child, err := sc.Child("profiling"); err == nil && !child.Has("enabled") {
			override.Enabled = def.Enabled
		}
		tier.Set(sub.Token, define.SubConfigFieldDefault, "", &override)
	}

	return tier, nil
}
