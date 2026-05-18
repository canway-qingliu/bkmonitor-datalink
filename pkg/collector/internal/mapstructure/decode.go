// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package mapstructure

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/mitchellh/mapstructure"
)

// Decode 默认支持 time.Duration 类型数据的转换处理
func Decode(input, output any) error {
	config := &mapstructure.DecoderConfig{
		Result: output,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			stringToBoolHookFunc(),
		),
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}

func stringToBoolHookFunc() mapstructure.DecodeHookFuncType {
	return func(from reflect.Type, to reflect.Type, data any) (any, error) {
		if from.Kind() != reflect.String || to.Kind() != reflect.Bool {
			return data, nil
		}

		parsed, err := strconv.ParseBool(strings.TrimSpace(data.(string)))
		if err != nil {
			return data, err
		}
		return parsed, nil
	}
}
