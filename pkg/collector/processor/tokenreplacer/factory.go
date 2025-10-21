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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/mapstructure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/processor"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

func init() {
	processor.Register(define.ProcessorTokenReplacer, NewFactory)
}

func NewFactory(conf map[string]any, customized []processor.SubConfigProcessor) (processor.Processor, error) {
	return newFactory(conf, customized)
}

func newFactory(conf map[string]any, customized []processor.SubConfigProcessor) (*tokenReplacer, error) {
	configs := confengine.NewTierConfig()
	c := &Config{}
	if err := mapstructure.Decode(conf, c); err != nil {
		return nil, err
	}
	c.Clean()
	configs.SetGlobal(*c)

	for _, custom := range customized {
		cfg := &Config{}
		if err := mapstructure.Decode(custom.Config.Config, cfg); err != nil {
			logger.Errorf("failed to decode config: %v", err)
			continue
		}
		cfg.Clean()
		configs.Set(custom.Token, custom.Type, custom.ID, *cfg)
	}

	return &tokenReplacer{
		CommonProcessor: processor.NewCommonProcessor(conf, customized),
		configs:         configs,
		replacer:        NewReplacer(),
	}, nil
}

type tokenReplacer struct {
	processor.CommonProcessor
	configs  *confengine.TierConfig // type: Config
	replacer Replacer
}

func (p *tokenReplacer) Name() string {
	return define.ProcessorTokenReplacer
}

func (p *tokenReplacer) IsDerived() bool {
	return false
}

func (p *tokenReplacer) IsPreCheck() bool {
	return true
}

func (p *tokenReplacer) Reload(config map[string]any, customized []processor.SubConfigProcessor) {
	f, err := newFactory(config, customized)
	if err != nil {
		logger.Errorf("failed to reload processor: %v", err)
		return
	}

	p.CommonProcessor = f.CommonProcessor
	p.configs = f.configs
	p.replacer = f.replacer
}

func (p *tokenReplacer) Process(record *define.Record) (*define.Record, error) {
	config := p.configs.GetByToken(record.Token.Original).(Config)

	var err error
	switch record.RecordType {
	case define.RecordTraces:
		err = p.processTraces(config, record)
	case define.RecordMetrics:
		err = p.processMetrics(config, record)
	case define.RecordLogs:
		err = p.processLogs(config, record)
	default:
		err = p.processCommon(config, record)
	}
	return nil, err
}

func tokenFromAttrs(attrs pcommon.Map, keys []string) string {
	for _, key := range keys {
		v, ok := attrs.Get(key)
		if ok {
			return v.AsString()
		}
	}
	return ""
}

func (p *tokenReplacer) processTraces(config Config, record *define.Record) error {
	pdTraces := record.Data.(ptrace.Traces)
	pdTraces.ResourceSpans().RemoveIf(func(resourceSpans ptrace.ResourceSpans) bool {
		p.replacer.replaceToken(config, record, resourceSpans.Resource().Attributes())
		return false
	})
	return nil
}

func (p *tokenReplacer) processMetrics(config Config, record *define.Record) error {
	pdMetrics := record.Data.(pmetric.Metrics)
	pdMetrics.ResourceMetrics().RemoveIf(func(resourceMetrics pmetric.ResourceMetrics) bool {
		p.replacer.replaceToken(config, record, resourceMetrics.Resource().Attributes())
		return false
	})
	return nil
}

func (p *tokenReplacer) processLogs(config Config, record *define.Record) error {
	pdLogs := record.Data.(plog.Logs)
	pdLogs.ResourceLogs().RemoveIf(func(resourceLogs plog.ResourceLogs) bool {
		p.replacer.replaceToken(config, record, resourceLogs.Resource().Attributes())
		return false
	})
	return nil
}

func (p *tokenReplacer) processCommon(config Config, record *define.Record) error {
	if replace, ok := config.replaceMapping[record.Token.Original]; ok {
		record.Token.Original = replace
	}
	return nil
}
