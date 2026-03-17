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
	"testing"
)

// BenchmarkConversionLogs 基准测试：日志转换
func BenchmarkConversionLogs(b *testing.B) {
	converter := NewOtelConverter()
	event := RUMEventV2{
		Type:      "view",
		EventType: "page_view",
		Date:      1000,
		Data:      map[string]interface{}{"message": "Page loaded"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = converter.ToOTEL(event)
	}
}

// BenchmarkConversionTraces 基准测试：跨度转换
func BenchmarkConversionTraces(b *testing.B) {
	converter := NewOtelConverter()
	event := RUMEventV2{
		Type:      "error",
		EventType: "javascript_error",
		Date:      1000,
		Data:      map[string]interface{}{"message": "Error occurred"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = converter.ToOTEL(event)
	}
}

// BenchmarkConversionMetrics 基准测试：指标转换
func BenchmarkConversionMetrics(b *testing.B) {
	converter := NewOtelConverter()
	event := RUMEventV2{
		Type:      "performance",
		EventType: "resource_timing",
		Date:      1000,
		Data: map[string]interface{}{
			"resource": map[string]interface{}{
				"duration": 150.0,
				"size":     2048.0,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = converter.ToOTEL(event)
	}
}

// BenchmarkBuilderPattern 基准测试：Builder 模式
func BenchmarkBuilderPattern(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewLogRecordBuilder(1000).
			SetBody("Test").
			SetSeverity("INFO").
			AddAttribute("key", "value").
			Build()
	}
}

// BenchmarkParallelConversion 基准测试：并发转换
func BenchmarkParallelConversion(b *testing.B) {
	converter := NewOtelConverter()
	event := RUMEventV2{
		Type:      "view",
		EventType: "page_view",
		Date:      1000,
		Data:      map[string]interface{}{"message": "Page"},
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = converter.ToOTEL(event)
		}
	})
}
