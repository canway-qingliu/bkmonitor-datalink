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
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/tokenparser"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/utils"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/pipeline"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/receiver"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

const (
	routeRumV1  = "/api/v2/rum"
	routeRumV2  = "/api/v2/rum/events"
	routeReplay = "/api/v2/replay"
)

func init() {
	receiver.RegisterReadyFunc(define.SourceDatadog, Ready)
}

var (
	metricMonitor = receiver.DefaultMetricMonitor.Source(define.SourceDatadog)
	otelConverter = NewOtelConverter()
)

func Ready() {
	receiver.RegisterRecvHttpRoute(define.SourceDatadog, []receiver.RouteWithFunc{
		{
			Method:       http.MethodPost,
			RelativePath: routeRumV1,
			HandlerFunc:  httpSvc.RumV1,
		},
		{
			Method:       http.MethodPost,
			RelativePath: routeRumV2,
			HandlerFunc:  httpSvc.RumV2,
		},
		{
			Method:       http.MethodPost,
			RelativePath: routeReplay,
			HandlerFunc:  httpSvc.RumV2,
		},
	})
}

type HttpService struct {
	receiver.Publisher
	pipeline.Validator
}

type convertedRecord struct {
	rtype define.RecordType
	data  interface{}
}

var httpSvc HttpService

// convertDataToDatadogEventV2 将原始数据转换为 RUMEventV2 格式
func convertDataToDatadogEventV2(data interface{}) (RUMEventV2, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return RUMEventV2{}, errors.New("data must be a map")
	}

	event := RUMEventV2{}

	// 提取基本字段
	if v, ok := m["type"].(string); ok {
		event.Type = v
	}
	if v, ok := m["event_type"].(string); ok {
		event.EventType = v
	}
	if v, ok := getNumberValue(m, "date"); ok {
		event.Date = int64(v)
	} else if v, ok := getNumberValue(m, "timestamp"); ok {
		event.Date = int64(v)
	}

	// 提取顶层属性
	if v, ok := m["source"].(string); ok {
		event.Source = v
	}
	if v, ok := m["service"].(string); ok {
		event.Service = v
	}
	if v, ok := m["version"].(string); ok {
		event.Version = v
	}
	if v, ok := m["ddtags"].(string); ok {
		event.DDTags = v
	}

	// 提取嵌套对象（保持完整的 map 结构）
	event.DD = getDDData(m, "_dd")
	event.Application = getMapValue(m, "application")
	event.View = getViewData(m, "view")
	event.Session = getMapValue(m, "session")
	event.Connectivity = getMapValue(m, "connectivity")
	event.User = getMapValue(m, "usr")
	event.Display = getMapValue(m, "display")

	// 根据事件类型提取相应的详细信息对象
	switch event.Type {
	case "resource":
		event.Resource = getResourceData(m, "resource")
	case "error":
		event.Error = getMapValue(m, "error")
	case "action":
		event.Action = getMapValue(m, "action")
	case "long_task":
		event.LongTask = getMapValue(m, "long_task")
	case "view":
		// view 信息已经在顶层的 View 字段
	default:
		// 其他未知类型，尝试从 data 字段提取
		event.Data = getMapValue(m, "data")
	}

	// 提取 meta 和 context（如果存在）
	event.Meta = getMapValue(m, "meta")
	event.Context = getMapValue(m, "context")

	return event, nil
}

// getMapValue 从 map 中获取 map 类型的值。
func getMapValue(m map[string]interface{}, key string) map[string]interface{} {
	if v, ok := m[key].(map[string]interface{}); ok {
		return v
	}
	return nil
}

// getNumberValue 从 map 中获取数值类型的值。
func getNumberValue(m map[string]interface{}, key string) (float64, bool) {
	value, ok := m[key].(float64)
	if !ok {
		return 0, false
	}

	return value, true
}

// getResourceData 从 map 中提取并转换为 ResourceData 结构
func getResourceData(m map[string]interface{}, key string) *ResourceData {
	resourceMap := getMapValue(m, key)
	if resourceMap == nil {
		return nil
	}

	resource := &ResourceData{}

	// 基本字段
	if v, ok := resourceMap["id"].(string); ok {
		resource.ID = v
	}
	if v, ok := resourceMap["type"].(string); ok {
		resource.Type = v
	}
	if v, ok := resourceMap["url"].(string); ok {
		resource.URL = v
	}
	if v, ok := resourceMap["duration"].(float64); ok {
		resource.Duration = int64(v)
	}

	// HTTP 状态码
	if v, ok := resourceMap["status_code"].(float64); ok {
		resource.StatusCode = int(v)
	}

	if v, ok := resourceMap["method"].(string); ok {
		resource.Method = v
	}

	// 传输信息
	if v, ok := resourceMap["delivery_type"].(string); ok {
		resource.DeliveryType = v
	}
	if v, ok := resourceMap["render_blocking_status"].(string); ok {
		resource.RenderBlockingStatus = v
	}

	// 大小信息
	if v, ok := resourceMap["size"].(float64); ok {
		resource.Size = int64(v)
	}
	if v, ok := resourceMap["encoded_body_size"].(float64); ok {
		resource.EncodedBodySize = int64(v)
	}
	if v, ok := resourceMap["decoded_body_size"].(float64); ok {
		resource.DecodedBodySize = int64(v)
	}
	if v, ok := resourceMap["transfer_size"].(float64); ok {
		resource.TransferSize = int64(v)
	}

	// 协议信息
	if v, ok := resourceMap["protocol"].(string); ok {
		resource.Protocol = v
	}

	// 时间详情
	resource.DNS = getResourceTiming(resourceMap, "dns")
	resource.Connect = getResourceTiming(resourceMap, "connect")
	resource.FirstByte = getResourceTiming(resourceMap, "first_byte")
	resource.Download = getResourceTiming(resourceMap, "download")

	return resource
}

// getResourceTiming 从 map 中提取并转换为 ResourceTiming 结构
func getResourceTiming(m map[string]interface{}, key string) *ResourceTiming {
	timingMap := getMapValue(m, key)
	if timingMap == nil {
		return nil
	}

	timing := &ResourceTiming{}

	if v, ok := timingMap["start"].(float64); ok {
		timing.Start = int64(v)
	}
	if v, ok := timingMap["duration"].(float64); ok {
		timing.Duration = int64(v)
	}

	// 若 start 和 duration 都为零，则返回 nil
	if timing.Start == 0 && timing.Duration == 0 {
		return nil
	}

	return timing
}

// getCounter 从 map 中提取计数信息
func getCounter(m map[string]interface{}, key string) *Counter {
	counterMap := getMapValue(m, key)
	if counterMap == nil {
		return nil
	}

	counter := &Counter{}
	if v, ok := counterMap["count"].(float64); ok {
		counter.Count = int(v)
	}

	return counter
}

// getViewData 从 map 中提取并转换为 ViewData 结构
func getViewData(m map[string]interface{}, key string) *ViewData {
	viewMap := getMapValue(m, key)
	if viewMap == nil {
		return nil
	}

	view := &ViewData{}

	// 基本信息
	if v, ok := viewMap["url"].(string); ok {
		view.URL = v
	}
	if v, ok := viewMap["referrer"].(string); ok {
		view.Referrer = v
	}
	if v, ok := viewMap["id"].(string); ok {
		view.ID = v
	}
	if v, ok := viewMap["is_active"].(bool); ok {
		view.IsActive = v
	}

	// 用户交互计数
	view.Action = getCounter(viewMap, "action")
	view.Error = getCounter(viewMap, "error")
	view.LongTask = getCounter(viewMap, "long_task")
	view.Resource = getCounter(viewMap, "resource")
	view.Frustration = getCounter(viewMap, "frustration")

	// 布局移位信息
	if v, ok := viewMap["cumulative_layout_shift"].(float64); ok {
		view.CumulativeLayoutShift = v
	}
	if v, ok := viewMap["cumulative_layout_shift_time"].(float64); ok {
		view.CumulativeLayoutShiftTime = int64(v)
	}
	if v, ok := viewMap["cumulative_layout_shift_target_selector"].(string); ok {
		view.CumulativeLayoutShiftTarget = v
	}

	// 加载性能指标（纳秒）
	if v, ok := viewMap["first_byte"].(float64); ok {
		view.FirstByte = int64(v)
	}
	if v, ok := viewMap["dom_interactive"].(float64); ok {
		view.DOMInteractive = int64(v)
	}
	if v, ok := viewMap["dom_content_loaded"].(float64); ok {
		view.DOMContentLoaded = int64(v)
	}
	if v, ok := viewMap["dom_complete"].(float64); ok {
		view.DOMComplete = int64(v)
	}
	if v, ok := viewMap["load_event"].(float64); ok {
		view.LoadEvent = int64(v)
	}
	if v, ok := viewMap["first_contentful_paint"].(float64); ok {
		view.FirstContentfulPaint = int64(v)
	}
	if v, ok := viewMap["largest_contentful_paint"].(float64); ok {
		view.LargestContentfulPaint = int64(v)
	}

	// 交互性能指标（纳秒）
	if v, ok := viewMap["interaction_to_next_paint"].(float64); ok {
		view.InteractionToNextPaint = int64(v)
	}
	if v, ok := viewMap["interaction_to_next_paint_time"].(float64); ok {
		view.InteractionToNextPaintTime = int64(v)
	}
	if v, ok := viewMap["interaction_to_next_paint_target_selector"].(string); ok {
		view.InteractionToNextPaintTarget = v
	}

	// 加载类型和时间
	if v, ok := viewMap["loading_type"].(string); ok {
		view.LoadingType = v
	}
	if v, ok := viewMap["loading_time"].(float64); ok {
		view.LoadingTime = int64(v)
	}
	if v, ok := viewMap["time_spent"].(float64); ok {
		view.TimeSpent = int64(v)
	}

	// 最大内容绘制目标选择器
	if v, ok := viewMap["largest_contentful_paint_target_selector"].(string); ok {
		view.LargestContentfulPaintTarget = v
	}

	// 性能指标详情
	view.Performance = getViewPerformanceMetrics(viewMap)

	return view
}

// getViewPerformanceMetrics 从 map 中提取性能指标详情
func getViewPerformanceMetrics(viewMap map[string]interface{}) *ViewPerformanceMetrics {
	perfMap := getMapValue(viewMap, "performance")
	if perfMap == nil {
		return nil
	}

	// 通过 JSON 序列化和反序列化来处理内联结构体类型匹配问题
	perfBytes, err := json.Marshal(perfMap)
	if err != nil {
		logger.Debugf("marshal performance failed: %v", err)
		return nil
	}

	metrics := &ViewPerformanceMetrics{}
	if err := json.Unmarshal(perfBytes, metrics); err != nil {
		logger.Debugf("unmarshal performance failed: %v", err)
		return nil
	}

	return metrics
}

// getDDData 从 map 中提取并转换为 DDData 结构
func getDDData(m map[string]interface{}, key string) *DDData {
	ddMap := getMapValue(m, key)
	if ddMap == nil {
		return nil
	}

	ddBytes, err := json.Marshal(ddMap)
	if err != nil {
		logger.Debugf("marshal DDData failed: %v", err)
		return nil
	}

	ddData := &DDData{}
	if err := json.Unmarshal(ddBytes, ddData); err != nil {
		logger.Debugf("unmarshal DDData failed: %v", err)
		return nil
	}

	return ddData
}

// transformRecord 将原始数据转换为 OTEL 格式。
func transformRecord(data interface{}) (ConversionResult, error) {
	// 先转换为 RUMEventV2
	event, err := convertDataToDatadogEventV2(data)
	if err != nil {
		return ConversionResult{}, err
	}

	// 日志输出转换过程
	logger.Warnf("convertedRUMEventV2: type=%s, event_type=%s, has_data=%v", event.Type, event.EventType, event.Data != nil)

	// 使用转换器进行转换
	conversionResult := otelConverter.ToOTEL(&event)

	return conversionResult, nil
}

// splitConversionResult 按信号类型拆分转换结果。
func splitConversionResult(result ConversionResult) []convertedRecord {
	records := make([]convertedRecord, 0, 3)

	if result.Logs.LogRecordCount() > 0 {
		records = append(records, convertedRecord{rtype: define.RecordLogs, data: result.Logs})
	}
	if result.Traces.SpanCount() > 0 {
		records = append(records, convertedRecord{rtype: define.RecordTraces, data: result.Traces})
	}
	if result.Metrics.MetricCount() > 0 {
		records = append(records, convertedRecord{rtype: define.RecordMetrics, data: result.Metrics})
	}

	return records
}

// debugLogConversionResult 使用 pdata 官方 JSON marshaler 输出调试日志。
func debugLogConversionResult(conversionResult ConversionResult) {
	debugLogPDataLogs(conversionResult.Logs)
	debugLogPDataTraces(conversionResult.Traces)
	debugLogPDataMetrics(conversionResult.Metrics)
}

// debugLogPDataLogs 输出 logs 调试内容。
func debugLogPDataLogs(logs plog.Logs) {
	if logs.LogRecordCount() == 0 {
		return
	}

	jsonBytes, err := plog.NewJSONMarshaler().MarshalLogs(logs)
	if err != nil {
		logger.Debugf("marshal pdata logs failed: %v", err)
		return
	}

	logger.Debugf("conversionResult.logs=%s", string(jsonBytes))
}

// debugLogPDataTraces 输出 traces 调试内容。
func debugLogPDataTraces(traces ptrace.Traces) {
	if traces.SpanCount() == 0 {
		return
	}

	jsonBytes, err := ptrace.NewJSONMarshaler().MarshalTraces(traces)
	if err != nil {
		logger.Debugf("marshal pdata traces failed: %v", err)
		return
	}

	logger.Debugf("conversionResult.traces=%s", string(jsonBytes))
}

// debugLogPDataMetrics 输出 metrics 调试内容。
func debugLogPDataMetrics(metrics pmetric.Metrics) {
	if metrics.MetricCount() == 0 {
		return
	}

	jsonBytes, err := pmetric.NewJSONMarshaler().MarshalMetrics(metrics)
	if err != nil {
		logger.Debugf("marshal pdata metrics failed: %v", err)
		return
	}

	logger.Debugf("conversionResult.metrics=%s", string(jsonBytes))
}

// publishConvertedRecords 按 conversionResult 分流发布 logs、traces 和 metrics。
func (s HttpService) publishConvertedRecords(conversionResult ConversionResult, ip string, token string, bodySize int, start time.Time) {
	logger.Infof(
		"Converted pdata result: logs=%d spans=%d metrics=%d",
		conversionResult.Logs.LogRecordCount(),
		conversionResult.Traces.SpanCount(),
		conversionResult.Metrics.MetricCount(),
	)

	debugLogConversionResult(conversionResult)

	for _, item := range splitConversionResult(conversionResult) {
		r := &define.Record{
			RequestType:   define.RequestHttp,
			RequestClient: define.RequestClient{IP: ip},
			RecordType:    item.rtype,
			Data:          item.data,
			Token:         define.Token{Original: token},
		}

		code, processorName, err := s.Validate(r)
		if err != nil {
			err = errors.Wrapf(err, "run pre-check failed, rtype=%s, code=%d, ip=%s", item.rtype.S(), code, ip)
			logger.WarnRate(time.Minute, r.Token.Original, err)
			metricMonitor.IncPreCheckFailedCounter(define.RequestHttp, item.rtype, processorName, r.Token.Original, code)
			continue
		}

		// s.Publish(r)
		receiver.RecordHandleMetrics(metricMonitor, r.Token, define.RequestHttp, item.rtype, bodySize, start)
	}
}

func (s HttpService) RumV1(w http.ResponseWriter, req *http.Request) {
	defer utils.HandleCrash()
	ip := utils.ParseRequestIP(req.RemoteAddr, req.Header)

	start := time.Now()
	buf := &bytes.Buffer{}
	_, err := io.Copy(buf, req.Body)
	if err != nil {
		metricMonitor.IncInternalErrorCounter(define.RequestHttp, define.RecordLogs)
		receiver.WriteResponse(w, define.ContentTypeJson, http.StatusInternalServerError, nil)
		logger.Errorf("failed to read datadog rum body: %v", err)
		return
	}
	defer func() {
		_ = req.Body.Close()
	}()

	// 记录接收到的原始数据
	dataBytes := buf.Bytes()
	logger.Infof("RumV1: received %d bytes data: %s", len(dataBytes), string(dataBytes[:min(len(dataBytes), 500)]))

	records, err := parseDatadogRUM(dataBytes)
	if err != nil {
		logger.Warnf("failed to parse datadog rum exported content, ip=%v, err: %v", ip, err)
		metricMonitor.IncDroppedCounter(define.RequestHttp, define.RecordLogs)
		receiver.WriteErrResponse(w, define.ContentTypeJson, http.StatusBadRequest, err)
		return
	}

	token := tokenparser.FromHttpRequest(req)

	for idx, data := range records {
		logger.Warnf("RumV1: processing record %d, data type=%T", idx, data)
		if m, ok := data.(map[string]interface{}); ok {
			logger.Warnf("RumV1: record is map with %d keys", len(m))
		}

		// 转换数据为 OTEL 格式
		conversionResult, err := transformRecord(data)
		if err != nil {
			logger.Warnf("RumV1: failed to transform record %d: %v", idx, err)
			metricMonitor.IncDroppedCounter(define.RequestHttp, define.RecordLogs)
			continue
		}
		// 打印 josn 内容
		s.publishConvertedRecords(conversionResult, ip, token, buf.Len(), start)
	}

	ddRequestID := req.URL.Query().Get("dd-request-id")
	response := map[string]string{"request_id": ddRequestID}
	responseBuf, _ := json.Marshal(response)
	receiver.WriteResponse(w, define.ContentTypeJson, http.StatusOK, responseBuf)
}

func (s HttpService) RumV2(w http.ResponseWriter, req *http.Request) {
	defer utils.HandleCrash()
	ip := utils.ParseRequestIP(req.RemoteAddr, req.Header)

	start := time.Now()
	buf := &bytes.Buffer{}
	_, err := io.Copy(buf, req.Body)
	if err != nil {
		metricMonitor.IncInternalErrorCounter(define.RequestHttp, define.RecordLogs)
		receiver.WriteResponse(w, define.ContentTypeJson, http.StatusInternalServerError, nil)
		logger.Errorf("failed to read datadog rum v2 body: %v", err)
		return
	}
	defer func() {
		_ = req.Body.Close()
	}()

	// 记录接收到的原始数据
	dataBytes := buf.Bytes()
	logger.Infof("RumV2: received %d bytes data: %s", len(dataBytes), string(dataBytes[:min(len(dataBytes), 500)]))

	records, err := parseDatadogRUMV2(dataBytes)
	if err != nil {
		logger.Warnf("failed to parse datadog rum v2 exported content, ip=%v, err: %v", ip, err)
		metricMonitor.IncDroppedCounter(define.RequestHttp, define.RecordLogs)
		receiver.WriteErrResponse(w, define.ContentTypeJson, http.StatusBadRequest, err)
		return
	}

	token := tokenparser.FromHttpRequest(req)

	for idx, data := range records {
		logger.Warnf("RumV2: processing record %d, data type=%T", idx, data)
		if m, ok := data.(map[string]interface{}); ok {
			logger.Warnf("RumV2: record is map with %d keys", len(m))
		}

		// 转换数据为 OTEL 格式
		conversionResult, err := transformRecord(data)
		if err != nil {
			logger.Warnf("RumV2: failed to transform record %d: %v", idx, err)
			metricMonitor.IncDroppedCounter(define.RequestHttp, define.RecordLogs)
			continue
		}

		s.publishConvertedRecords(conversionResult, ip, token, buf.Len(), start)
	}

	ddRequestID := req.URL.Query().Get("dd-request-id")
	response := map[string]string{"request_id": ddRequestID}
	responseBuf, _ := json.Marshal(response)
	receiver.WriteResponse(w, define.ContentTypeJson, http.StatusOK, responseBuf)
}
