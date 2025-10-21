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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/metacache"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
	"strings"
)

const (
	typeTokenMapping = "token_mapping"
	typeAppName      = "app_name"
)

type Config struct {
	Type         string   `config:"type" mapstructure:"type"`
	ResourceKey  string   `config:"resource_key" mapstructure:"resource_key"`
	resourceKeys []string // 避免多次转换开销

	// type: token_mapping
	replaceMapping map[string]string
	ReplaceList    []*ReplaceItem `config:"replace_list" mapstructure:"replace_list"`

	// type: app_name
	appTokenMapping map[string]define.Token
}

type ReplaceItem struct {
	Original string `config:"original" mapstructure:"original"`
	Replace  string `config:"replace" mapstructure:"replace"`
}

func (c *Config) Clean() {
	var keys []string
	for _, key := range strings.Split(c.ResourceKey, ",") {
		keys = append(keys, strings.TrimSpace(key))
	}
	c.resourceKeys = keys

	switch c.Type {
	case typeTokenMapping:
		c.cleanReplaceMapping()
	case typeAppName:
		c.cleanAppTokenMapping(metacache.Default)
	default:
		logger.Warnf("unknown tokenreplacer type: %s, skipping initialization", c.Type)
	}
}

func (c *Config) cleanReplaceMapping() {
	replaceMapping := make(map[string]string)
	for _, item := range c.ReplaceList {
		replaceMapping[item.Original] = item.Replace
	}
	c.replaceMapping = replaceMapping
}

func (c *Config) cleanAppTokenMapping(cache *metacache.Cache) {
	appTokenMapping := make(map[string]define.Token)

	// TODO(aivan): 此处暂不考虑不同业务下存在相同 app_name 的情况（即同名应用会覆盖），后续如有需求再做调整
	cache.Range(func(key string, value define.Token) bool {
		if value.AppName == "" {
			return true
		}
		appTokenMapping[value.AppName] = value
		return true
	})

	c.appTokenMapping = appTokenMapping
}
