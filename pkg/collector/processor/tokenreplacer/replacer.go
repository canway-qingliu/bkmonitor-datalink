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
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func NewReplacer() Replacer {
	return Replacer{}
}

type Replacer struct{}

func (p *Replacer) replaceToken(config Config, record *define.Record, attrs pcommon.Map) {
	src := []string{
		record.Token.Original,
		tokenFromAttrs(attrs, config.resourceKeys),
	}
	for _, token := range src {
		switch config.Type {
		case typeAppName:
			if replace, ok := config.appTokenMapping[token]; ok {
				record.Token.Original = replace.Original
				break
			}
		default:
			if replace, ok := config.replaceMapping[token]; ok {
				record.Token.Original = replace
				break
			}
		}
	}
}
