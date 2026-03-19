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
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

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

// 常量配置
const (
	maxScanLineSize = 1024 * 1024 // 单行最大 1MB
	maxParseLines   = 10000       // 最大解析行数，防攻击
)

// 支持 NDJSON / 单对象 / 数组 自动识别
func parseDatadogRUM(buf []byte) ([]interface{}, error) {
	// panic 保护
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("parseDatadogRUM panic: %v\nstack: %s", r, debug.Stack())
		}
	}()

	// 打印前 N 字符
	preview := string(buf[:min(len(buf), 300)])
	logger.Infof("parseDatadogRUM: data length=%d, preview: %s", len(buf), preview)

	records, ndjsonSuccess, stats := tryParseNDJSON(buf)
	if ndjsonSuccess {
		logger.Infof("parse success: format=NDJSON, total=%d, success=%d, failed=%d, skipped=%d",
			stats.totalLines, stats.successLines, stats.failedLines, stats.skippedLines)
		return records, nil
	}

	// 试解析为单个对象 / 数组
	var data interface{}
	if err := json.Unmarshal(buf, &data); err != nil {
		logger.Errorf("json unmarshal failed: %v", err)
		return nil, fmt.Errorf("invalid json format: %w", err)
	}

	switch v := data.(type) {
	case map[string]interface{}:
		logger.Infof("parse success: format=single object, keys=%d", len(v))
		return []interface{}{v}, nil

	case []interface{}:
		logger.Infof("parse success: format=array, items=%d", len(v))
		return v, nil

	default:
		return nil, errors.New("unsupported data type: not map/array/ndjson")
	}
}

// parseStats 解析统计信息
type parseStats struct {
	totalLines   int
	successLines int
	failedLines  int
	skippedLines int
}

// tryParseNDJSON 流式解析 NDJSON
func tryParseNDJSON(buf []byte) ([]interface{}, bool, parseStats) {
	var records []interface{}
	var stats parseStats

	scanner := bufio.NewScanner(bytes.NewReader(buf))
	scanner.Buffer(make([]byte, maxScanLineSize), maxScanLineSize) // 支持超长行

	for scanner.Scan() {
		stats.totalLines++
		// 防过载
		if stats.totalLines > maxParseLines {
			logger.Warnf("parse stop: exceed max line limit %d", maxParseLines)
			break
		}

		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			stats.skippedLines++
			continue
		}

		var item interface{}
		if err := json.Unmarshal(line, &item); err != nil {
			stats.failedLines++
			logger.Debugf("line %d parse failed: %v", stats.totalLines, err)
			continue
		}

		records = append(records, item)
		stats.successLines++
	}

	// 扫描器错误
	if err := scanner.Err(); err != nil {
		logger.Debugf("ndjson scan error: %v", err)
		return nil, false, stats
	}

	// 解析到有效记录才算成功
	return records, len(records) > 0, stats
}

// parseDatadogRUMV2 解析 Datadog Browser SDK RUM 数据
func parseDatadogRUMV2(buf []byte) ([]interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("parseDatadogRUMV2 panic: %v\nstack: %s", r, debug.Stack())
		}
	}()

	logger.Infof("parseDatadogRUMV2: data length=%d", len(buf))

	// 首先尝试流式解析 JSON Lines 格式
	records, success, stats := tryParseNDJSON(buf)
	if success {
		logger.Infof("parse v2 success: format=NDJSON, total=%d, success=%d, failed=%d, skipped=%d",
			stats.totalLines, stats.successLines, stats.failedLines, stats.skippedLines)
		return records, nil
	}

	// 失败则尝试作为单个 map 或数组
	var data interface{}
	if err := json.Unmarshal(buf, &data); err != nil {
		logger.Errorf("json unmarshal failed: %v", err)
		return nil, fmt.Errorf("invalid datadog rum v2 format: %w", err)
	}

	switch v := data.(type) {
	case map[string]interface{}:
		logger.Infof("parse v2 success: format=single object, keys=%d", len(v))
		return []interface{}{v}, nil
	case []interface{}:
		logger.Infof("parse v2 success: format=array, items=%d", len(v))
		return v, nil
	default:
		return nil, errors.New("unsupported data type in rum v2: not map/array/ndjson")
	}
}
