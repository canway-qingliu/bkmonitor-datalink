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
	"strings"

	"github.com/pkg/errors"
)

const wildcardType = "*"

type Config struct {
	MeterData MeterDataAction `config:"meter_data" mapstructure:"meter_data"`
}

func (c *Config) Validate() error {
	return c.MeterData.Validate()
}

type MeterDataAction struct {
	Enabled bool         `config:"enabled" mapstructure:"enabled"`
	Rules   []MetricRule `config:"rules" mapstructure:"rules"`

	exactMappings    map[string]map[string]string
	wildcardMappings map[string]string
}

type MetricRule struct {
	Types   []string          `config:"types" mapstructure:"types"`
	Metrics map[string]string `config:"metrics" mapstructure:"metrics"`
}

func (a *MeterDataAction) Validate() error {
	a.exactMappings = make(map[string]map[string]string)
	a.wildcardMappings = make(map[string]string)
	if a.Enabled && len(a.Rules) == 0 {
		return errors.New("meter_data action: no rules specified")
	}

	for i, rule := range a.Rules {
		if len(rule.Types) == 0 {
			return errors.Errorf("meter_data action: rules[%d].types is required", i)
		}
		if len(rule.Metrics) == 0 {
			return errors.Errorf("meter_data action: rules[%d].metrics is required", i)
		}

		hasWildcard := false
		for _, framework := range rule.Types {
			if strings.TrimSpace(framework) == wildcardType {
				hasWildcard = true
				break
			}
		}
		if hasWildcard && len(rule.Types) != 1 {
			return errors.Errorf("meter_data action: rules[%d].types cannot mix wildcard and exact values", i)
		}

		for _, framework := range rule.Types {
			framework = strings.TrimSpace(framework)
			if framework == "" {
				return errors.Errorf("meter_data action: rules[%d].types contains empty value", i)
			}

			for source, destination := range rule.Metrics {
				source = strings.TrimSpace(source)
				destination = strings.TrimSpace(destination)
				if source == "" || destination == "" {
					return errors.Errorf("meter_data action: rules[%d].metrics contains empty source or destination", i)
				}

				if framework == wildcardType {
					if _, ok := a.wildcardMappings[source]; ok {
						return errors.Errorf("meter_data action: duplicated mapping type=%q metric=%q", framework, source)
					}
					a.wildcardMappings[source] = destination
					continue
				}

				mappings := a.exactMappings[framework]
				if mappings == nil {
					mappings = make(map[string]string)
					a.exactMappings[framework] = mappings
				}
				if _, ok := mappings[source]; ok {
					return errors.Errorf("meter_data action: duplicated mapping type=%q metric=%q", framework, source)
				}
				mappings[source] = destination
			}
		}
	}
	return nil
}

func (a MeterDataAction) Match(framework, metric string) (string, bool) {
	framework = strings.TrimSpace(framework)
	if framework == "" {
		return "", false
	}
	if mappings := a.exactMappings[framework]; mappings != nil {
		if destination, ok := mappings[metric]; ok {
			return destination, true
		}
	}
	destination, ok := a.wildcardMappings[metric]
	return destination, ok
}
