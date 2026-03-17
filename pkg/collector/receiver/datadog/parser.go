// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package datadog

import (
	"bytes"
	"encoding/json"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
	"github.com/pkg/errors"
)

// RUMEvent 已弃用 - 保留用于向后兼容
// 新代码应使用 RUMEventV2
// Deprecated: Use RUMEventV2 instead
type RUMEvent struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data,omitempty"`
	Meta map[string]interface{} `json:"meta,omitempty"`
}

// RUMEventV2 表示 Datadog RUM V2 格式的事件结构
// 保留完整的 Datadog RUM 事件属性
type RUMEventV2 struct {
	// 基本字段
	Type      string `json:"type"`
	EventType string `json:"event_type,omitempty"`
	Date      int64  `json:"date"`

	// 顶层属性
	Source  string `json:"source,omitempty"`  // browser, ios, android
	Service string `json:"service,omitempty"` // 服务名称
	Version string `json:"version,omitempty"` // 版本号
	DDTags  string `json:"ddtags,omitempty"`  // 标签字符串

	// 嵌套对象（保持为 map 以支持动态结构）
	DD           map[string]interface{} `json:"_dd,omitempty"`          // Datadog 元数据
	Application  map[string]interface{} `json:"application,omitempty"`  // 应用信息
	View         map[string]interface{} `json:"view,omitempty"`         // 视图信息
	Session      map[string]interface{} `json:"session,omitempty"`      // 会话信息
	Connectivity map[string]interface{} `json:"connectivity,omitempty"` // 网络连接信息
	User         map[string]interface{} `json:"usr,omitempty"`          // 用户信息
	Display      map[string]interface{} `json:"display,omitempty"`      // 显示信息
	Resource     map[string]interface{} `json:"resource,omitempty"`     // 资源详细信息
	Error        map[string]interface{} `json:"error,omitempty"`        // 错误信息
	Action       map[string]interface{} `json:"action,omitempty"`       // 用户操作信息
	LongTask     map[string]interface{} `json:"long_task,omitempty"`    // 长任务信息

	// 兼容旧字段（用于特殊场景）
	Data    map[string]interface{} `json:"data,omitempty"`
	Meta    map[string]interface{} `json:"meta,omitempty"`
	Context map[string]interface{} `json:"context,omitempty"`
}

// RUMEventV2 表示 Datadog RUM V2 格式的事件结构
// V1 格式通常是单个 JSON 对象或数组
func parseDatadogRUM(buf []byte) ([]interface{}, error) {
	var records []interface{}

	// 日志记录原始数据
	logger.Warnf("parseDatadogRUM: data length=%d, first 300 chars: %s", len(buf), string(buf[:min(len(buf), 300)]))

	// 首先尝试按行解析 NDJSON 格式 (最可能)
	// 支持 \n 和 \r\n 两种行分隔符
	lines := bytes.Split(buf, []byte("\n"))
	jsonLineCount := 0
	for i, line := range lines {
		// 移除 \r（如果存在）
		line = bytes.TrimSuffix(line, []byte("\r"))

		// 跳过空行
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		// 直接解析为通用对象
		var data interface{}
		if err := json.Unmarshal(line, &data); err != nil {
			logger.Debugf("line %d parse failed: %v", i, err)
			continue
		}

		// 转换为 map，便于后续处理
		if m, ok := data.(map[string]interface{}); ok {
			logger.Warnf("line %d parsed successfully, type: %T, keys: %v", i, data, getMapKeys(m))
			records = append(records, m)
			jsonLineCount++
		} else {
			logger.Warnf("line %d parsed but not a map, type: %T, value: %v", i, data, data)
			records = append(records, data)
			jsonLineCount++
		}
	}

	if len(records) > 0 {
		logger.Warnf("✓ parsed %d objects from NDJSON format", jsonLineCount)
		return records, nil
	}

	// 如果不是 NDJSON，尝试作为单个对象或数组
	var data interface{}
	if err := json.Unmarshal(buf, &data); err != nil {
		logger.Errorf("json.Unmarshal failed: %v", err)
		return nil, errors.New("invalid datadog rum format")
	}

	// 根据类型处理
	switch v := data.(type) {
	case map[string]interface{}:
		// 单个对象
		logger.Warnf("✓ parsed as single map with %d keys", len(v))
		records = append(records, v)
		return records, nil
	case []interface{}:
		// 数组
		logger.Warnf("✓ parsed as array with %d items", len(v))
		for _, item := range v {
			records = append(records, item)
		}
		return records, nil
	default:
		logger.Errorf("✗ unsupported type: %T", data)
		return nil, errors.New("invalid datadog rum format")
	}
}

// getMapKeys 获取 map 的所有 key
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// parseDatadogRUMV2 解析 Datadog Browser SDK RUM 数据 (V2 格式 - 推荐)
// V2 格式是 JSON Lines 格式 (每行一个 JSON 对象)
func parseDatadogRUMV2(buf []byte) ([]interface{}, error) {
	var records []interface{}

	// 日志记录原始数据
	logger.Warnf("parseDatadogRUMV2: data length=%d, first 300 chars: %s", len(buf), string(buf[:min(len(buf), 300)]))

	// 首先尝试按行解析 (JSON Lines 格式)
	// 支持 \n 和 \r\n 两种行分隔符
	lines := bytes.Split(buf, []byte("\n"))
	for i, line := range lines {
		// 移除 \r（如果存在）
		line = bytes.TrimSuffix(line, []byte("\r"))

		// 跳过空行
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		// 直接解析为通用对象
		var data interface{}
		if err := json.Unmarshal(line, &data); err != nil {
			logger.Debugf("failed to parse line %d: %v", i, err)
			continue
		}
		records = append(records, data)
	}

	if len(records) > 0 {
		logger.Warnf("✓ parsed %d events from NDJSON format", len(records))
		return records, nil
	}

	// 如果不是 JSON Lines，尝试作为单个 map 或数组
	var data interface{}
	if err := json.Unmarshal(buf, &data); err != nil {
		logger.Errorf("json.Unmarshal failed: %v", err)
		return nil, errors.New("invalid datadog rum v2 format")
	}

	switch v := data.(type) {
	case map[string]interface{}:
		logger.Warnf("✓ parsed as single map with %d keys", len(v))
		records = append(records, v)
		return records, nil
	case []interface{}:
		logger.Warnf("✓ parsed as array with %d items", len(v))
		for _, item := range v {
			records = append(records, item)
		}
		return records, nil
	default:
		logger.Errorf("unsupported type: %T", data)
		return nil, errors.New("invalid datadog rum v2 format")
	}
}
