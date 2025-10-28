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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
)

func tokenFromAttrs(attrs pcommon.Map, keys []string) (string, string, bool) {
	for _, key := range keys {
		v, ok := attrs.Get(key)
		if ok {
			return key, v.AsString(), true
		}
	}
	return "", "", false
}

func NewReplacer() Replacer {
	return Replacer{}
}

type Replacer struct{}

func (p *Replacer) replaceToken(config Config, record *define.Record, attrs pcommon.Map) {
	attrKey, attrValue, found := tokenFromAttrs(attrs, config.resourceKeys)

	switch config.Type {
	case typeAppName:
		if found && attrValue != "" {
			if replace, ok := config.appTokenMapping[attrValue]; ok {
				attrs.UpdateString(attrKey, replace.Original)
				record.Token.Original = replace.Original
			}
		} else if replace, ok := config.appTokenMapping[record.Token.Original]; ok {
			record.Token.Original = replace.Original
		}
	default:
		if found && attrValue != "" {
			if replace, ok := config.replaceMapping[attrValue]; ok {
				attrs.UpdateString(attrKey, replace)
				record.Token.Original = replace
			}
		} else if replace, ok := config.replaceMapping[record.Token.Original]; ok {
			record.Token.Original = replace
		}
	}
}
